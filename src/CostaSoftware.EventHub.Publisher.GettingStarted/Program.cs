using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.Publisher.GettingStarted
{
    public class Program
    {
        private const string ConnectionStrin_EnvVariable = "EASTBANCTECH_EVENTHUBCONNSTRING";

        static async Task Main(string[] args)
        {
            var connectionString = "";
            var eventHubName = "snowiq-eventhub";
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // properties of the EventHubProducerClient
            var producerOption = new EventHubProducerClientOptions()
            {
                ConnectionOptions = new EventHubConnectionOptions()
                {
                    TransportType = EventHubsTransportType.AmqpWebSockets
                }
            };
            var producer = new EventHubProducerClient(connectionString, eventHubName, producerOption);
            //await DisplayProducerProperties(producer);
            
            

            // properties of the EventHubConsumerClient
            var consumerOption = new EventHubConsumerClientOptions();
            consumerOption.ConnectionOptions.TransportType = EventHubsTransportType.AmqpWebSockets;
            var consumer = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName, consumerOption);
            //await DisplayConsumerProperties(consumer);



            // Conceptually, the consumer group is a label that identifies one or more event consumers as a set.
            // Often, consumer groups are named after the responsibility of the consumer in an application, such as "Telemetry" or "OrderProcessing"
            // EventHubConsumerClient with ConsumerGroup
            // properties of the EventHubConsumerClient
            var eventHubConnection = new EventHubConnection(connectionString, eventHubName, new EventHubConnectionOptions());
            var orderProcessingConsumer = new EventHubConsumerClient("OrderProcessing", eventHubConnection);
            //await DisplayConsumerProperties(orderProcessingConsumer);

            // authentication in client
            // identity - based authorization
            //// EventHubsConnectionStringProperties connectionStringProperties = EventHubsConnectionStringProperties.Parse(connectionString);
            //// var credential = new DefaultAzureCredential();
            //// var fullyQualifiedNamespace = "<< NAMESPACE (likely similar to {your-namespace}.servicebus.windows.net) >>";
            //// var producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential);

            // Shared Access Signature authorization
            //// var credential = new AzureSasCredential("<< SHARED ACCESS KEY STRING >>");
            //// var fullyQualifiedNamespace = "<< NAMESPACE (likely similar to {your-namespace}.servicebus.windows.net) >>";
            //// var producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential);

            // Shared Access Key authorization
            //// var credential = new AzureNamedKeyCredential("<< SHARED KEY NAME >>", "<< SHARED KEY >>");
            //// var fullyQualifiedNamespace = "<< NAMESPACE (likely similar to {your-namespace}.servicebus.windows.net) >>";
            //// var producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential);

            // Parsing a connection string for information
            //// TokenCredential credential = new DefaultAzureCredential();
            //// var authenticatedProducerClient = new EventHubProducerClient(
            ////     connectionStringProperties.FullyQualifiedNamespace,
            ////     connectionStringProperties.EventHubName ?? eventHubName,
            ////     credential);

            //await ProduceEvents(producer);
            //await ConsumeEvents(consumer);
            //await ConsumeEvents(testingConsumer);

            var learnProcesso = new EventHubsProcessorLearn();
            await learnProcesso.ProduceEvents();
            await learnProcesso.Process1();
        }

        private async static Task DisplayConsumerProperties(EventHubConsumerClient consumer)
        {
            var consumerProperties = await consumer.GetEventHubPropertiesAsync();
            Console.WriteLine("The Event Hub has the following properties:");
            Console.WriteLine($"\tThe path to the Event Hub from the namespace is: { consumerProperties.Name }");
            Console.WriteLine($"\tThe Event Hub was created at: { consumerProperties.CreatedOn }, in UTC.");
            Console.WriteLine($"\tThe following partitions are available: [{ string.Join(", ", consumerProperties.PartitionIds) }]");
            var partitions = await consumer.GetPartitionIdsAsync();
            Console.WriteLine($"The following partitions are available: [{ string.Join(", ", partitions) }]");
            var firstPartition = partitions.FirstOrDefault();
            var partitionProperties = await consumer.GetPartitionPropertiesAsync(firstPartition);
            Console.WriteLine($"Partition: { partitionProperties.Id }");
            Console.WriteLine($"\tThe partition contains no events: { partitionProperties.IsEmpty }");
            Console.WriteLine($"\tThe first sequence number is: { partitionProperties.BeginningSequenceNumber }");
            Console.WriteLine($"\tThe last sequence number is: { partitionProperties.LastEnqueuedSequenceNumber }");
            Console.WriteLine($"\tThe last offset is: { partitionProperties.LastEnqueuedOffset }");
            Console.WriteLine($"\tThe last enqueued time is: { partitionProperties.LastEnqueuedTime }, in UTC.");
        }

        private static async Task ProduceEvents(EventHubProducerClient producer, bool useParticionKey = false)
        {
            try
            {
                // using ParticionId
                string firstPartition = (await producer.GetPartitionIdsAsync()).First();
                var specificPartionBatchOption = new CreateBatchOptions() { PartitionId = firstPartition };

                // using PartitionKey
                using EventDataBatch eventDataBatch = useParticionKey ? await producer.CreateBatchAsync(new CreateBatchOptions() { PartitionKey = "TEST" }) : await producer.CreateBatchAsync();

                for (int counter = 0; counter < int.MaxValue; ++counter)
                {
                    var eventBody = new BinaryData($"Event Number : {counter}");
                    var eventData = new EventData(eventBody);
                    // Custom Metadata
                    eventData.Properties.Add("EventType", "com.microsoft.samples.hello-event");
                    eventData.Properties.Add("priority", 1);
                    eventData.Properties.Add("score", 9.0);

                    if (!eventDataBatch.TryAdd(eventData))
                    {
                        break;
                    }
                }

                await producer.SendAsync(eventDataBatch);
                //await producer.SendAsync(new List<EventData>() { new EventData("event body") });

            }
            catch
            {
            }
            finally
            {
                await producer.DisposeAsync();
            }
        }

        private static async Task ConsumeEvents(EventHubConsumerClient consumer)
        {
            try
            {
                // To ensure that we do not wait for an indeterminate length of time, we'll
                // stop reading after we receive five events.  For a fresh Event Hub, those
                // will be the first five that we had published.  We'll also ask for
                // cancellation after 90 seconds, just to be safe.

                using var cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(90));

                var maximumEvents = 5;
                int eventsRead = 0;
                var eventDataRead = new List<string>();
                int loopTicks = 0;
                int maximumTicks = 10;

                var options = new ReadEventOptions
                {
                    MaximumWaitTime = TimeSpan.FromSeconds(1)
                };

                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(options))
                //await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(cancellationSource.Token))
                {
                    if (partitionEvent.Data != null)
                    {
                        string readFromPartition = partitionEvent.Partition.PartitionId;
                        byte[] eventBodyBytes = partitionEvent.Data.EventBody.ToArray();
                        Console.WriteLine($"Read event of length { eventBodyBytes.Length } from { readFromPartition }");

                        eventsRead++;
                        eventDataRead.Add(partitionEvent.Data.EventBody.ToString());
                    }
                    else
                    {
                        Console.WriteLine("wait time elapsed; no event was available");
                    }

                    loopTicks++;

                    if (loopTicks >= maximumTicks)
                    {
                        break;
                    }

                    //if (eventDataRead.Count >= maximumEvents)
                    if (eventsRead >= maximumEvents)
                    {
                        break;
                    }
                }
                // At this point, the data sent as the body of each event is held
                // in the eventDataRead set.


                foreach (var eventData in eventDataRead)
                {
                    Console.WriteLine(eventData);
                }

            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            catch
            {
                // Transient failures will be automatically retried as part of the
                // operation. If this block is invoked, then the exception was either
                // fatal or all retries were exhausted without a successful read.
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }

        private static async Task DisplayProducerProperties(EventHubProducerClient producer)
        {
            var properties = await producer.GetEventHubPropertiesAsync();
            Console.WriteLine("The Event Hub has the following properties:");
            Console.WriteLine($"\tThe path to the Event Hub from the namespace is: { properties.Name }");
            Console.WriteLine($"\tThe Event Hub was created at: { properties.CreatedOn }, in UTC.");
            Console.WriteLine($"\tThe following partitions are available: [{ string.Join(", ", properties.PartitionIds) }]");
            string[] partitions = await producer.GetPartitionIdsAsync();
            Console.WriteLine($"The following partitions are available: [{ string.Join(", ", partitions) }]");
            string firstPartition = partitions.FirstOrDefault();
            PartitionProperties partitionProperties = await producer.GetPartitionPropertiesAsync(firstPartition);
            Console.WriteLine($"Partition: { partitionProperties.Id }");
            Console.WriteLine($"\tThe partition contains no events: { partitionProperties.IsEmpty }");
            Console.WriteLine($"\tThe first sequence number is: { partitionProperties.BeginningSequenceNumber }");
            Console.WriteLine($"\tThe last sequence number is: { partitionProperties.LastEnqueuedSequenceNumber }");
            Console.WriteLine($"\tThe last offset is: { partitionProperties.LastEnqueuedOffset }");
            Console.WriteLine($"\tThe last enqueued time is: { partitionProperties.LastEnqueuedTime }, in UTC.");
        }

    }
}
