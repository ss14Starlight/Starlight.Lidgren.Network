using System.Collections.Generic;

namespace Lidgren.Network;

public partial class NetPeer
{
	internal sealed class ReceivedFragmentGroup
	{
		// Starlight-start: chunks are stored as they arrive instead of preallocating TotalBytes up front,
		// so a single tiny fragment claiming a 16 MiB group no longer allocates 16 MiB
		public Dictionary<int, (byte[] Data, int Length)> Chunks { get; } = new();
		// Starlight-end
		public int TotalBytes { get; }
		public int TotalBits { get; }
		public int ChunkByteSize { get; }
		public int TotalNumChunks { get; }
		public double LastReceived { get; set; }
		// Starlight-start
		public int StoredBytes { get; private set; }
		public int ReceivedChunkCount => Chunks.Count;
		// Starlight-end

		public ReceivedFragmentGroup(int totalBytes, int totalBits, int chunkByteSize, int totalNumChunks, double lastReceived) // Starlight-edit
		{
			TotalBytes = totalBytes;
			TotalBits = totalBits;
			ChunkByteSize = chunkByteSize;
			TotalNumChunks = totalNumChunks;
			LastReceived = lastReceived;
		}

		// Starlight-start

		public bool HasChunk(int chunkNumber)
			=> Chunks.ContainsKey(chunkNumber);

		public void AddChunk(int chunkNumber, byte[] data, int length, int cost)
		{
			Chunks.Add(chunkNumber, (data, length));
			StoredBytes += cost;
		}

		// Starlight-end
	}
}
