using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.WorkerService.Subscriber
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
                    var eventHubConfig = hostContext.Configuration.GetSection("EventHub").GetChildren().ToList();
                    var eventHubConnectionString = eventHubConfig.Where(c => c.Key.Equals("ConnectionString")).FirstOrDefault().Value;
                    var eventHub = eventHubConfig.Where(c => c.Key.Equals("EventHub")).FirstOrDefault().Value;
                    var consumerGroup = eventHubConfig.Where(c => c.Key.Equals("ConsumerGroup")).FirstOrDefault().Value;

                    var blobStorage = hostContext.Configuration.GetSection("BlobStorage").GetChildren().ToList();
                    var blobStorageConnectionString = blobStorage.Where(c => c.Key.Equals("ConnectionString")).FirstOrDefault().Value;
                    var container = blobStorage.Where(c => c.Key.Equals("Container")).FirstOrDefault().Value;

                    services.AddHostedService<SubscriberWorker>();
                    BlobContainerClient blobContainerClient = new BlobContainerClient(blobStorageConnectionString, container);
                    var eventProcessorClientOptions = new EventProcessorClientOptions() { ConnectionOptions = new EventHubConnectionOptions() { TransportType = EventHubsTransportType.AmqpWebSockets } };
                    services.AddSingleton<EventProcessorClient>(new EventProcessorClient(blobContainerClient, consumerGroup, eventHubConnectionString, eventProcessorClientOptions));
                });
    }
}
