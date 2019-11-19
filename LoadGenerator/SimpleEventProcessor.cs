using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace LoadGenerator
{
    public class SimpleEventProcessor : IEventProcessor
    {
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Processor Shutting Down. Partition '{context.PartitionId}', Reason: '{reason}'.");
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"SimpleEventProcessor initialized. Partition: '{context.PartitionId}'");
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.WriteLine($"Error on Partition: {context.PartitionId}, Error: {error.Message}");
            return Task.CompletedTask;
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                Console.WriteLine($"Message received. Partition: '{context.PartitionId}' >>>>>>>>>>>>>>>>>>> ");

                foreach (var property in eventData.Properties)
                {
                    Console.WriteLine($"Message received. Partition: '{context.PartitionId}', Property: '{property}'");
                }


                var data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                try
                {
                    JToken parsedJson = JToken.Parse(data);
                    var beautified = parsedJson.ToString(Formatting.Indented);
                    Console.WriteLine($"Message received. Partition: '{context.PartitionId}', fdr: '{beautified}'");
                }
                catch (Exception)
                {
                    Console.WriteLine($"Message received. Partition: '{context.PartitionId}', fdr: '{data}'");
                }
                Console.WriteLine($"Message received. Partition: '{context.PartitionId}' <<<<<<<<<<<<<<<<<< ");
            }
            await context.CheckpointAsync();
        }
    }
}
