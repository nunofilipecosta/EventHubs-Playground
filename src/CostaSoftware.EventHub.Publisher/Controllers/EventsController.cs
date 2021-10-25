using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.Publisher.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EventsController : ControllerBase
    {
        private readonly EventHubProducerClient producerClient;
        private readonly Random random;

        public EventsController(EventHubProducerClient producerClient, Random random)
        {
            this.producerClient = producerClient;
            this.random = random;
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            EventData eventData;

            try
            {
                var createBatchOptions = new CreateBatchOptions();
                // for a specific partition
                ////createBatchOptions.PartitionId = (await this.producerClient.GetPartitionIdsAsync()).FirstOrDefault();
                ///
                // for grouping the events in a partition but we don't what wich one
                ////createBatchOptions.PartitionKey = "testingPartion";

                using EventDataBatch eventDataBatch = await producerClient.CreateBatchAsync(createBatchOptions);

                eventData = new EventData(Encoding.UTF8.GetBytes($"Event Number : {random.Next(1, 100)}"));

                // Custom Metadata
                eventData.Properties.Add("EventType", "com.microsoft.samples.hello-event");
                eventData.Properties.Add("priority", 1);
                eventData.Properties.Add("score", 9.0);

                if (!eventDataBatch.TryAdd(eventData))
                {
                    return BadRequest(eventData);
                }

                await producerClient.SendAsync(eventDataBatch);
            }
            catch
            {
                throw;
            }
            

            return Ok(eventData.EventBody.ToString());

        }




        [HttpPost]
        public async Task<IActionResult> Post()
        {
            await Task.CompletedTask;
            return Ok();
        }
    }
}
