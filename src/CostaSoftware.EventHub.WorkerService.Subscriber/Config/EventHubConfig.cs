using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CostaSoftware.EventHub.WorkerService.Subscriber.Config
{
    internal class EventHubConfig
    {
        public string ConnectionString { get; set; }
        public string EventHub { get; set; }

        public string ConsumerGroup { get; set; }
    }

    internal class BlobStorageConfig
    {
        public string ConnectionString { get; set; }
        public string Container { get; set; }
    }
}
