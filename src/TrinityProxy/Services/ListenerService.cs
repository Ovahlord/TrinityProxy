using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using TrinityProxy.Configuration;
using TrinityProxy.IO;

namespace TrinityProxy.Services;

public class ListenerService : IHostedService
{
    private readonly List<ProxyListener> _proxyListeners = [];
    
    public ListenerService(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    
    private readonly IConfiguration _configuration;
    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        List<ProxyListenerSettings>? proxyListenerSettings = _configuration
            .GetSection("ProxyListeners")
            .Get<List<ProxyListenerSettings>>();
        
        if (proxyListenerSettings == null || proxyListenerSettings.Count == 0)
            return;

        foreach (ProxyListenerSettings listenerSettings in proxyListenerSettings)
        {
            Console.WriteLine("Started listener");
            ProxyListener listener = new(listenerSettings);
            _proxyListeners.Add(listener);
            listener.Start();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (ProxyListener listener in _proxyListeners)
        {
            listener.Stop();
        }
    }
}