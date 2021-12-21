using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.WorkerService.Subscriber
{
    public class SubscriberWorker : BackgroundService
    {
        private readonly ILogger<SubscriberWorker> _logger;
        private readonly EventProcessorClient _eventProcessorClient;

        public SubscriberWorker(ILogger<SubscriberWorker> logger, EventProcessorClient eventProcessorClient)
        {
            _logger = logger;
            _eventProcessorClient = eventProcessorClient;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {


            while (!stoppingToken.IsCancellationRequested)
            {


                try
                {
                    _eventProcessorClient.ProcessEventAsync += ProcessEventHandler;
                    _eventProcessorClient.ProcessErrorAsync += ProcessErrorHandler;
                    _eventProcessorClient.PartitionInitializingAsync += Processor_PartitionInitializingAsync;
                    _eventProcessorClient.PartitionClosingAsync += Processor_PartitionClosingAsync;

                    await _eventProcessorClient.StartProcessingAsync(stoppingToken);
                    await Task.Delay(Timeout.Infinite, stoppingToken);
                    //await Task.Delay(10000, stoppingToken);
                }
                catch (TaskCanceledException taskCanceledException)
                {
                    Console.WriteLine(taskCanceledException.Message);
                    // This is expected if the cancellation token is
                    // signaled.
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                finally
                {
                    // It is encouraged that you unregister your handlers when you have
                    // finished using the Event Processor to ensure proper cleanup.  This
                    // is especially important when using lambda expressions or handlers
                    // in any form that may contain closure scopes or hold other references.

                    _eventProcessorClient.ProcessEventAsync -= ProcessEventHandler;
                    _eventProcessorClient.ProcessErrorAsync -= ProcessErrorHandler;
                    _eventProcessorClient.PartitionInitializingAsync -= Processor_PartitionInitializingAsync;
                    _eventProcessorClient.PartitionClosingAsync -= Processor_PartitionClosingAsync;

                    // This may take up to the length of time defined
                    // as part of the configured TryTimeout of the processor;
                    // by default, this is 60 seconds.

                    await _eventProcessorClient.StopProcessingAsync();
                }
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
            //Console.WriteLine($"Processor_PartitionInitializingAsync : {args.DefaultStartingPosition}");
            Console.WriteLine($"Processor_PartitionInitializingAsync : {args.PartitionId}");

            //EventPosition startPositionWhenNoCheckpoint = EventPosition.Earliest;
            //args.DefaultStartingPosition = startPositionWhenNoCheckpoint;

            return Task.CompletedTask;
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



                Console.WriteLine($"Event received: { Encoding.UTF8.GetString(eventBody) } from partition {args.Partition.PartitionId}");

                Console.WriteLine($"Properties");
                foreach (var item in args.Data.Properties)
                {
                    Console.WriteLine($"{item.Key} - {item.Value}");
                }

                Console.WriteLine($"System Properties");
                foreach (var item in args.Data.SystemProperties)
                {
                    Console.WriteLine($"{item.Key} - {item.Value}");
                }

                Console.WriteLine($"Content Type : {args.Data.ContentType}");
                Console.WriteLine($"CorrelationId : {args.Data.CorrelationId}");
                Console.WriteLine($"MessageId : {args.Data.MessageId}");
                Console.WriteLine($"PartitionKey : {args.Data.PartitionKey}");

                await args.UpdateCheckpointAsync();

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
            catch (Exception exception)
            {
                // It is very important that you always guard against
                // exceptions in your handler code; the processor does
                // not have enough understanding of your code to
                // determine the correct action to take.  Any
                // exceptions from your handlers go uncaught by
                // the processor and will NOT be redirected to
                // the error handler.

                Console.WriteLine(exception);
            }
        }
    }
}
