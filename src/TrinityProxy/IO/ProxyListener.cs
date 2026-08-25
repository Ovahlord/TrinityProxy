using System.Net;
using System.Net.Sockets;
using System.Threading.RateLimiting;
using TrinityProxy.Configuration;

namespace TrinityProxy.IO;

public class ProxyListener
{
    private readonly IPEndPoint _clientEndPoint;
    private readonly IPEndPoint _serverEndPoint;
    private readonly SlidingWindowRateLimiterOptions _streamRateLimiterOptions;
    private readonly SlidingWindowRateLimiter _connectionRateLimiter;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ProxyListenerSettings _proxyListenerSettings;
    
    public ProxyListener(ProxyListenerSettings proxyListenerSettings)
    {
        _clientEndPoint = IPEndPoint.Parse(proxyListenerSettings.ClientIpEndpoint);
        _serverEndPoint = IPEndPoint.Parse(proxyListenerSettings.ServerIpEndpoint);
        _proxyListenerSettings = proxyListenerSettings;
        
        _streamRateLimiterOptions = new SlidingWindowRateLimiterOptions()
        {
            AutoReplenishment = true,
            QueueLimit = 0,
            SegmentsPerWindow = 4,
            PermitLimit = proxyListenerSettings.MaxBytesPerSecond,
            Window = TimeSpan.FromSeconds(1),
        };

        _connectionRateLimiter = new(new SlidingWindowRateLimiterOptions()
        {
            AutoReplenishment = true,
            QueueLimit = 0,
            SegmentsPerWindow = 4,
            PermitLimit = proxyListenerSettings.ConnectionsPerSecond,
            Window = TimeSpan.FromSeconds(1),
        });

        _cancellationTokenSource =  new CancellationTokenSource();
    }

    public void Start()
    {
        // Bind synchronously so that a failure to claim the port surfaces to the caller
        // instead of disappearing into the discarded accept loop task.
        TcpListener listener = new(_clientEndPoint);
        listener.Start(_proxyListenerSettings.ConnectionBacklogSize);

        _ = ListenForClientsAsync(listener, _cancellationTokenSource.Token);
    }

    public void Stop()
    {
        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();
    }

    private async Task ListenForClientsAsync(TcpListener listener, CancellationToken cancellationToken = default)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient clientClient = await listener.AcceptTcpClientAsync(cancellationToken);
                RateLimitLease lease = await _connectionRateLimiter.AcquireAsync(1, cancellationToken);
                if (!lease.IsAcquired)
                {
                    // Read the endpoint before closing: TcpClient.Client is null once the client is closed.
                    EndPoint? deniedEndPoint = clientClient.Client.RemoteEndPoint;
                    clientClient.Close();
                    Console.WriteLine($"New connection denied due to exceeding rate limit {deniedEndPoint}");
                    continue;
                }

                // Bridging runs off the accept loop so that a slow or unreachable server
                // neither blocks new clients nor tears the listener down when it fails.
                _ = EstablishConnectionAsync(clientClient, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            // The accept loop runs detached, so anything unexpected has to be reported here
            // or the listener would go quiet with no trace of why.
            Console.WriteLine($"Listener {_clientEndPoint} stopped unexpectedly: {exception.Message}");
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task EstablishConnectionAsync(TcpClient clientClient, CancellationToken cancellationToken)
    {
        RateLimitedStream? clientStream = null;
        TcpClient? serverClient = null;

        try
        {
            clientStream = new RateLimitedStream(clientClient.GetStream(), _streamRateLimiterOptions,
                _proxyListenerSettings.MaxBytesPerRead, _proxyListenerSettings.CloseConnectionWhenRateExceeded)
            {
                ReadTimeout = _proxyListenerSettings.ReceiveTimeout
            };

            Console.WriteLine($"Attempting to connect to Server: {_serverEndPoint}");
            serverClient = new TcpClient();
            await serverClient.ConnectAsync(_serverEndPoint, cancellationToken);

            Console.WriteLine($"New connection established with server {serverClient.Client.RemoteEndPoint}");

            // The bridge takes ownership of both streams once it is constructed.
            new ProxyBridge(clientStream, serverClient.GetStream());
        }
        catch (Exception exception)
        {
            if (exception is not OperationCanceledException)
                Console.WriteLine($"Failed to connect to server {_serverEndPoint}: {exception.Message}");

            clientStream?.Dispose();
            serverClient?.Dispose();
            clientClient.Dispose();
        }
    }
}