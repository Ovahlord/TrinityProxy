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
        _ = ListenForClientsAsync(_cancellationTokenSource.Token);
    }

    public void Stop()
    {
        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();
    }

    private async Task ListenForClientsAsync(CancellationToken cancellationToken = default)
    {
        TcpListener listener = new(_clientEndPoint);
        listener.Start(_proxyListenerSettings.ConnectionBacklogSize);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient clientClient = await listener.AcceptTcpClientAsync(cancellationToken);
                RateLimitLease lease = await _connectionRateLimiter.AcquireAsync(1, cancellationToken);
                if (!lease.IsAcquired)
                {
                    clientClient.Close();
                    Console.WriteLine($"New connection denied due to exceeding rate limit {clientClient.Client.RemoteEndPoint}");
                    continue;
                }

                RateLimitedStream clientStream = new(clientClient.GetStream(), _streamRateLimiterOptions,
                    _proxyListenerSettings.MaxBytesPerRead, _proxyListenerSettings.CloseConnectionWhenRateExceeded)
                {
                    ReadTimeout = _proxyListenerSettings.ReceiveTimeout
                };
                    
                Console.WriteLine($"Attempting to connect to Server: {_serverEndPoint}");
                TcpClient serverClient = new();
                await serverClient.ConnectAsync(_serverEndPoint, cancellationToken);
                    
                Console.WriteLine($"New connection established with server {serverClient.Client.RemoteEndPoint}");
                    
                new ProxyBridge(clientStream, serverClient.GetStream());
                
            }
        }
        catch (OperationCanceledException)
        {
            
        }       
        finally 
        {
            listener.Stop();
        }
    }
}