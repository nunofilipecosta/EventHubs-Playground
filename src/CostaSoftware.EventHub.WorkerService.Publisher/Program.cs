using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.WorkerService.Publisher
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>();

                    var eventHubConfig = hostContext.Configuration.GetSection("EventHub").GetChildren().ToList();
                    var eventHubConnectionString = eventHubConfig.Where(c => c.Key.Equals("ConnectionString")).FirstOrDefault().Value;
                    var eventHub = eventHubConfig.Where(c => c.Key.Equals("EventHub")).FirstOrDefault().Value;

                    services.AddSingleton<EventHubProducerClient>((IServiceProvider) => {
                        return new EventHubProducerClient(eventHubConnectionString, new EventHubProducerClientOptions() { Identifier = "testingId" });
                    });
                });
    }
}
