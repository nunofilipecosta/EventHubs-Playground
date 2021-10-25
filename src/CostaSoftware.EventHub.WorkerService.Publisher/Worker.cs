using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.WorkerService.Publisher
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly EventHubProducerClient eventHubProducerClient;

        public Worker(ILogger<Worker> logger, EventHubProducerClient eventHubProducerClient)
        {
            _logger = logger;
            this.eventHubProducerClient = eventHubProducerClient;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var random = new Random();
            while (!stoppingToken.IsCancellationRequested)
            {
                using EventDataBatch eventDataBatch = await eventHubProducerClient.CreateBatchAsync(new CreateBatchOptions() { });
                var eventData = new EventData(Encoding.UTF8.GetBytes($"Event Number : {random.Next(1, 100)} at time : {DateTime.Now.ToShortTimeString()}"));

                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
