using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Lidgren.Network;
using NUnit.Framework;

namespace UnitTests;

// Starlight: end to end check that real fragmented messages still reassemble with chunked reassembly storage
[TestFixture]
public sealed class LoopbackFragmentTests
{
	[Test]
	[TestCase(300 * 1024)]
	[TestCase(3 * 1024 * 1024 + 123)]
	public void LargeMessageRoundTrips(int size)
	{
		var serverConfig = new NetPeerConfiguration(nameof(LoopbackFragmentTests))
		{
			Port = 0,
			LocalAddress = IPAddress.Loopback,
		};
		var server = new NetServer(serverConfig);
		var client = new NetClient(new NetPeerConfiguration(nameof(LoopbackFragmentTests)) { LocalAddress = IPAddress.Loopback });
		server.Start();
		client.Start();

		try
		{
			var connection = client.Connect(new IPEndPoint(IPAddress.Loopback, server.Port));
			WaitFor(() => connection.Status == NetConnectionStatus.Connected, client, server);

			var payload = new byte[size];
			new Random(size).NextBytes(payload);

			var om = client.CreateMessage(size);
			om.Write(payload);
			client.SendMessage(om, NetDeliveryMethod.ReliableOrdered);

			byte[]? received = null;
			WaitFor(() =>
			{
				NetIncomingMessage? im;
				while ((im = server.ReadMessage()) != null)
				{
					if (im.MessageType == NetIncomingMessageType.Data)
						received = im.ReadBytes(im.LengthBytes);
					server.Recycle(im);
				}

				return received != null;
			}, client);

			Assert.That(received, Is.EqualTo(payload));
			Assert.That(server.Connections[0].m_fragmentReassemblyBytes, Is.Zero);
		}
		finally
		{
			client.Shutdown("done");
			server.Shutdown("done");
		}
	}

	private static void WaitFor(Func<bool> condition, params NetPeer[] drain)
	{
		var sw = Stopwatch.StartNew();
		while (!condition())
		{
			if (sw.Elapsed > TimeSpan.FromSeconds(30))
				Assert.Fail("Timed out");

			foreach (var peer in drain)
			{
				NetIncomingMessage? im;
				while ((im = peer.ReadMessage()) != null)
					peer.Recycle(im);
			}

			Thread.Sleep(5);
		}
	}
}
