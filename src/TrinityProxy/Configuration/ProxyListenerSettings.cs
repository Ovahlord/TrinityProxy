namespace TrinityProxy.Configuration;

public class ProxyListenerSettings
{
    public string ClientIpEndpoint { get; set; } = "127.0.0.1:8086";
    public string ServerIpEndpoint { get; set; } = "127.0.0.1:8087";
    public int MaxBytesPerSecond { get; set; } = 4096;
    public int MaxBytesPerRead { get; set; } = 1024;
    public int ReceiveTimeout { get; set; } = 6000;
    public int ConnectionBacklogSize { get; set; } = 500;
    public int ConnectionsPerSecond { get; set; } = 100;
}