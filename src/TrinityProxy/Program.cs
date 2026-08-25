
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TrinityProxy.Services;

namespace TrinityProxy;

class Program
{
    static async Task Main(string[] args)
    {
        var builder = Host.CreateEmptyApplicationBuilder(null);
        builder.Configuration.AddJsonFile("Settings.json");
        builder.Services.AddHostedService<ListenerService>();
        
        var app = builder.Build();
        await app.RunAsync();
    }
}
