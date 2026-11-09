using System;
using System.Threading;
using System.Collections.Generic;

namespace Lidgren.Network
{
	public partial class NetPeer
	{
		private int m_lastUsedFragmentGroup;

		// Starlight-start: bytes held by incomplete fragment groups across all connections, network thread only
		private long m_fragmentReassemblyBytes;
		// rough per chunk bookkeeping cost (dictionary entry + tuple) so tiny chunks are not free
		private const int c_fragmentChunkOverheadBytes = 64;
		// Starlight-end

		// on user thread
		private NetSendResult SendFragmentedMessage(NetOutgoingMessage msg, IList<NetConnection> recipients, NetDeliveryMethod method, int sequenceChannel)
		{
			// Note: this group id is PER SENDING/NetPeer; ie. same id is sent to all recipients;
			// this should be ok however; as long as recipients differentiate between same id but different sender
			int group = Interlocked.Increment(ref m_lastUsedFragmentGroup);
			if (group >= NetConstants.MaxFragmentationGroups)
			{
				// @TODO: not thread safe; but in practice probably not an issue
				m_lastUsedFragmentGroup = 1;
				group = 1;
			}
			msg.m_fragmentGroup = group;

			// do not send msg; but set fragmentgroup in case user tries to recycle it immediately

			// create fragmentation specifics
			int totalBytes = msg.LengthBytes;

			// determine minimum mtu for all recipients
			int mtu = GetMTU(recipients);
			int bytesPerChunk = NetFragmentationHelper.GetBestChunkSize(group, totalBytes, mtu);

			int numChunks = totalBytes / bytesPerChunk;
			if (numChunks * bytesPerChunk < totalBytes)
				numChunks++;

			NetSendResult retval = NetSendResult.Sent;

			int bitsPerChunk = bytesPerChunk * 8;
			int bitsLeft = msg.LengthBits;
			for (int i = 0; i < numChunks; i++)
			{
				NetOutgoingMessage chunk = CreateMessage(0);

				chunk.m_bitLength = (bitsLeft > bitsPerChunk ? bitsPerChunk : bitsLeft);
				chunk.m_data = msg.m_data;
				chunk.m_fragmentGroup = group;
				chunk.m_fragmentGroupTotalBits = totalBytes * 8;
				chunk.m_fragmentChunkByteSize = bytesPerChunk;
				chunk.m_fragmentChunkNumber = i;

				NetException.Assert(chunk.m_bitLength != 0);
				NetException.Assert(chunk.GetEncodedSize() < mtu);

				Interlocked.Add(ref chunk.m_recyclingCount, recipients.Count);

				foreach (NetConnection recipient in recipients)
				{
					var res = recipient.EnqueueMessage(chunk, method, sequenceChannel);
					if (res == NetSendResult.Dropped)
						Interlocked.Decrement(ref chunk.m_recyclingCount);
					if ((int)res > (int)retval)
						retval = res; // return "worst" result
				}

				bitsLeft -= bitsPerChunk;
			}

			return retval;
		}

		private void HandleReleasedFragment(NetIncomingMessage im)
		{
			VerifyNetworkThread();

			//
			// read fragmentation header and combine fragments
			//
			if (!NetFragmentationHelper.TryReadHeader(
				im.Data, 0, im.LengthBytes,
				out int ptr,
				out int group,
				out int totalBits,
				out int chunkByteSize,
				out int chunkNumber
			))
			{
				LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Dropping malformed fragment header from {im.SenderEndPoint}");
				Recycle(im);
				return;
			}

			NetException.Assert(im.LengthBytes > ptr);

			NetException.Assert(group > 0);
			NetException.Assert(totalBits > 0);
			NetException.Assert(chunkByteSize > 0);

			int totalBytes = NetUtility.BytesToHoldBits(totalBits);
			int payloadLength = im.LengthBytes - ptr;

			if (im.SenderConnection == null
				|| group <= 0
				|| totalBits <= 0
				|| chunkByteSize <= 0
				|| chunkByteSize > NetConstants.MaximumFragmentChunkSize
				|| totalBytes <= 0
				|| totalBytes > NetConstants.MaximumFragmentGroupSize
				|| payloadLength <= 0
				|| payloadLength > chunkByteSize)
			{
				LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Dropping malformed fragment from {im.SenderEndPoint} (group={group}, totalBits={totalBits}, chunkByteSize={chunkByteSize}, payload={payloadLength})");
				Recycle(im);
				return;
			}

			int totalNumChunks = (totalBytes + chunkByteSize - 1) / chunkByteSize;

			NetException.Assert(chunkNumber < totalNumChunks);

			// Every chunk but the last must be exactly chunkByteSize and the last must
			// be exactly the remainder. Otherwise tiny chunks could "complete" a large
			// group with little traffic, exposing stale pooled buffer contents.
			int expectedPayload = chunkNumber == totalNumChunks - 1
				? totalBytes - (totalNumChunks - 1) * chunkByteSize
				: chunkByteSize;

			if (chunkNumber < 0
				|| chunkNumber >= totalNumChunks
				|| payloadLength != expectedPayload)
			{
				LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Dropping out-of-range fragment {chunkNumber}/{totalNumChunks} from {im.SenderEndPoint}");
				Recycle(im);
				return;
			}

			NetException.Assert(im.SenderConnection != null);

			// Starlight-start
			var connection = im.SenderConnection;
			var groups = connection.m_receivedFragmentGroups;
			// Starlight-end
			double now = NetTime.Now;
			ExpireFragmentGroups(connection, now); // Starlight-edit

			// Starlight start: memory is accounted per received chunk, not per claimed group size,
			// so holding reassembly memory requires actually sending that much data
			int chunkCost = GetStorageBucketSize(payloadLength) + c_fragmentChunkOverheadBytes;
			if (connection.m_fragmentReassemblyBytes + chunkCost > m_configuration.m_maximumFragmentReassemblyBytesPerConnection
				|| m_fragmentReassemblyBytes + chunkCost > m_configuration.m_maximumFragmentReassemblyBytes)
			{
				LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Too much fragment reassembly data from {im.SenderEndPoint}; dropping fragment");
				Recycle(im);
				return;
			}
			// Starlight end

			if (!groups.TryGetValue(group, out var info))
			{
				// single fragment groups can't accumulate unbounded buffers
				if (groups.Count >= NetConstants.MaximumConcurrentFragmentGroups)
				{
					LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Too many concurrent fragment groups from {im.SenderEndPoint}; dropping fragment");
					Recycle(im);
					return;
				}

				// Starlight-start: a group that can never fit in the budget can never complete
				if (totalBytes > m_configuration.m_maximumFragmentReassemblyBytesPerConnection
					|| totalBytes > m_configuration.m_maximumFragmentReassemblyBytes)
				{
					LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Fragment group of {totalBytes} bytes from {im.SenderEndPoint} exceeds reassembly limit; dropping fragment");
					Recycle(im);
					return;
				}
				// Starlight-end

				info = new ReceivedFragmentGroup(
					totalBytes,
					totalBits,
					chunkByteSize,
					totalNumChunks,
					now);
				groups[group] = info;
			}
			// The computed offset/copy and received chunk set depend on this
			// header data matching the first fragment for the group.
			else if (info.TotalBytes != totalBytes
				|| info.TotalBits != totalBits
				|| info.ChunkByteSize != chunkByteSize
				|| info.TotalNumChunks != totalNumChunks)
			{
				LogRateLimitedWarning(NetLogRateLimitTarget.MalformedFragment, im.SenderEndPoint, $"Dropping inconsistent fragment for group {group} from {im.SenderEndPoint}");
				Recycle(im);
				return;
			}

			if (info.HasChunk(chunkNumber)) // Starlight-edit
			{
				Recycle(im);
				return;
			}

			info.LastReceived = now;

			// Starlight-start
			// copy chunk payload out; im is recycled or reused below
			byte[] chunkData = GetStorage(payloadLength);
			Buffer.BlockCopy(im.Data, ptr, chunkData, 0, payloadLength);
			info.AddChunk(chunkNumber, chunkData, payloadLength, chunkCost);
			connection.m_fragmentReassemblyBytes += chunkCost;
			m_fragmentReassemblyBytes += chunkCost;
			// Starlight-end

			int cnt = info.ReceivedChunkCount;

			LogVerbose($"Received fragment {chunkNumber} of {totalNumChunks} ({cnt} chunks received)");

			if (cnt == totalNumChunks)
			{
				// Starlight-start
				// Done! Assemble the chunks and transform this incoming message
				byte[] data = GetStorage(totalBytes);
				foreach (var pair in info.Chunks)
					Buffer.BlockCopy(pair.Value.Data, 0, data, pair.Key * chunkByteSize, pair.Value.Length);

				LogVerbose($"Fragment group #{group} fully received in {totalNumChunks} chunks ({totalBits} bits)");
				groups.Remove(group);
				ReleaseFragmentGroup(connection, info);

				Recycle(im.m_data!);
				im.m_data = data;
				im.m_bitLength = totalBits;
				im.m_isFragment = false;

				ReleaseMessage(im);
				// Starlight-end
			}
			else
			{
				// data has been copied; recycle this incoming message
				Recycle(im);
			}

			return;
		}

		internal void ExpireFragmentGroups(NetConnection connection, double now)
		{
			var groups = connection.m_receivedFragmentGroups; // Starlight-edit
			if (groups.Count == 0)
				return;

			double oldestAllowed = now - m_configuration.m_fragmentGroupTimeout;
			foreach (var pair in new List<KeyValuePair<int, ReceivedFragmentGroup>>(groups))
			{
				if (pair.Value.LastReceived >= oldestAllowed)
					continue;

				groups.Remove(pair.Key);
				ReleaseFragmentGroup(connection, pair.Value);
			}
		}

		#region Starlight
		internal void ClearFragmentGroups(NetConnection connection)
		{
			foreach (var group in connection.m_receivedFragmentGroups.Values)
				ReleaseFragmentGroup(connection, group);
			connection.m_receivedFragmentGroups.Clear();
		}

		private void ReleaseFragmentGroup(NetConnection connection, ReceivedFragmentGroup group)
		{
			foreach (var chunk in group.Chunks.Values)
				Recycle(chunk.Data);
			group.Chunks.Clear();

			connection.m_fragmentReassemblyBytes -= group.StoredBytes;
			m_fragmentReassemblyBytes -= group.StoredBytes;
		}
		#endregion
	}
}
