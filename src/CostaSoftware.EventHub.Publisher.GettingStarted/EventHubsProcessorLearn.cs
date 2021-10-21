using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.Publisher.GettingStarted
{
    internal class EventHubsProcessorLearn
    {
        private readonly string connectionString = "";
        private readonly string eventHubName = "snowiq-eventhub-processor";

        public EventHubsProcessorLearn()
        {

        }

        public async Task ProduceEvents()
        {
            var producer = new EventHubProducerClient(connectionString, eventHubName);

            try
            {
                using EventDataBatch eventBatch = await producer.CreateBatchAsync();

                for (var counter = 0; counter < 10; ++counter)
                {
                    var eventBody = new BinaryData($"This is an event with Number: { counter }");
                    var eventData = new EventData(eventBody);

                    if (!eventBatch.TryAdd(eventData))
                    {
                        // At this point, the batch is full but our last event was not
                        // accepted.  For our purposes, the event is unimportant so we
                        // will intentionally ignore it.  In a real-world scenario, a
                        // decision would have to be made as to whether the event should
                        // be dropped or published on its own.

                        break;
                    }
                }

                // When the producer publishes the event, it will receive an
                // acknowledgment from the Event Hubs service; so long as there is no
                // exception thrown by this call, the service assumes responsibility for
                // delivery.  Your event data will be published to one of the Event Hub
                // partitions, though there may be a (very) slight delay until it is
                // available to be consumed.

                await producer.SendAsync(eventBatch);
            }
            catch
            {
                // Transient failures will be automatically retried as part of the
                // operation. If this block is invoked, then the exception was either
                // fatal or all retries were exhausted without a successful publish.
            }
            finally
            {
                await producer.CloseAsync();
            }
        }

        public async Task Process1()
        {
            var storageConnectionString = "";
            var blobContainerName = "eventhubdata";

            var eventHubsConnectionString = "";
            var eventHubName = this.eventHubName;
            var consumerGroup = "tracking";

            var processorOptions = new EventProcessorClientOptions
            {
                ConnectionOptions = new EventHubConnectionOptions() { TransportType = EventHubsTransportType.AmqpWebSockets },
                LoadBalancingStrategy = LoadBalancingStrategy.Greedy,
                LoadBalancingUpdateInterval = TimeSpan.FromSeconds(10),
                PartitionOwnershipExpirationInterval = TimeSpan.FromSeconds(30),
                RetryOptions = new EventHubsRetryOptions()
                {
                    Mode = EventHubsRetryMode.Exponential,
                    MaximumRetries = 5,
                    Delay = TimeSpan.FromMilliseconds(800),
                    MaximumDelay = TimeSpan.FromSeconds(10)
                },
                
                
            };

            var storageClient = new BlobContainerClient(storageConnectionString, blobContainerName);
            var processor = new EventProcessorClient(storageClient, consumerGroup, eventHubsConnectionString, eventHubName);
            var processor2 = new EventProcessorClient(storageClient, consumerGroup, eventHubsConnectionString, eventHubName, processorOptions);

            var partitionEventCount = new ConcurrentDictionary<string, int>();



            try
            {
                using var cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(90));

                processor.ProcessEventAsync += ProcessEventHandler;
                processor.ProcessErrorAsync += ProcessErrorHandler;
                processor.PartitionInitializingAsync += Processor_PartitionInitializingAsync;
                processor.PartitionClosingAsync += Processor_PartitionClosingAsync;
                processor2.ProcessEventAsync += ProcessEventHandler;
                processor2.ProcessErrorAsync += ProcessErrorHandler;

                try
                {
                    await processor.StartProcessingAsync(cancellationSource.Token);
                    await processor2.StartProcessingAsync(cancellationSource.Token);
                    await Task.Delay(Timeout.Infinite, cancellationSource.Token);
                }
                catch (TaskCanceledException taskCanceledException)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(taskCanceledException.Message);
                    // This is expected if the cancellation token is
                    // signaled.
                }
                finally
                {
                    // This may take up to the length of time defined
                    // as part of the configured TryTimeout of the processor;
                    // by default, this is 60 seconds.

                    await processor.StopProcessingAsync();
                    await processor2.StopProcessingAsync();
                }
            }
            catch
            {
                // The processor will automatically attempt to recover from any
                // failures, either transient or fatal, and continue processing.
                // Errors in the processor's operation will be surfaced through
                // its error handler.
                //
                // If this block is invoked, then something external to the
                // processor was the source of the exception.
            }
            finally
            {
                // It is encouraged that you unregister your handlers when you have
                // finished using the Event Processor to ensure proper cleanup.  This
                // is especially important when using lambda expressions or handlers
                // in any form that may contain closure scopes or hold other references.

                processor.ProcessEventAsync -= ProcessEventHandler;
                processor.ProcessErrorAsync -= ProcessErrorHandler;
                processor.PartitionInitializingAsync-= Processor_PartitionInitializingAsync;
                processor.PartitionClosingAsync -= Processor_PartitionClosingAsync;
                processor2.ProcessEventAsync -= ProcessEventHandler;
                processor2.ProcessErrorAsync -= ProcessErrorHandler;
            }
        }

        private Task Processor_PartitionClosingAsync(PartitionClosingEventArgs args)
        {
            string description = args.Reason switch
            {
                ProcessingStoppedReason.OwnershipLost =>
                    "Another processor claimed ownership",

                ProcessingStoppedReason.Shutdown =>
                    "The processor is shutting down",

                _ => args.Reason.ToString()
            };

            Console.WriteLine($"Closing partition: { args.PartitionId }");
            Console.WriteLine($"\tReason: { description }");

            return Task.CompletedTask;
        }

        private Task Processor_PartitionInitializingAsync(PartitionInitializingEventArgs args)
        {
            EventPosition startPositionWhenNoCheckpoint = EventPosition.FromEnqueuedTime(DateTimeOffset.UtcNow);
            args.DefaultStartingPosition = startPositionWhenNoCheckpoint;

            return Task.CompletedTask;
        }

        private async Task ProcessEventHandler(ProcessEventArgs args)
        {
            try
            {
                // If the cancellation token is signaled, then the
                // processor has been asked to stop.  It will invoke
                // this handler with any events that were in flight;
                // these will not be lost if not processed.
                //
                // It is up to the handler to decide whether to take
                // action to process the event or to cancel immediately.

                if (args.CancellationToken.IsCancellationRequested)
                {
                    return;
                }

                string partition = args.Partition.PartitionId;
                byte[] eventBody = args.Data.EventBody.ToArray();
                Console.WriteLine($"Event from partition { partition } with length { eventBody.Length }.");

                //int eventsSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
                //    key: partition,
                //    addValue: 1,
                //    updateValueFactory: (_, currentCount) => currentCount + 1);

                //if (eventsSinceLastCheckpoint >= 50)
                //{
                //    await args.UpdateCheckpointAsync();
                //    partitionEventCount[partition] = 0;
                //}
            }
            catch
            {
                // It is very important that you always guard against
                // exceptions in your handler code; the processor does
                // not have enough understanding of your code to
                // determine the correct action to take.  Any
                // exceptions from your handlers go uncaught by
                // the processor and will NOT be redirected to
                // the error handler.
            }
        }

        private Task ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            try
            {
                Console.WriteLine("Error in the EventProcessorClient");
                Console.WriteLine($"\tOperation: { args.Operation }");
                Console.WriteLine($"\tException: { args.Exception }");
                Console.WriteLine("");
            }
            catch
            {
                // It is very important that you always guard against
                // exceptions in your handler code; the processor does
                // not have enough understanding of your code to
                // determine the correct action to take.  Any
                // exceptions from your handlers go uncaught by
                // the processor and will NOT be handled in any
                // way.
            }

            return Task.CompletedTask;
        }
    }
}
