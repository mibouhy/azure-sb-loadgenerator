using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventGrid;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace LoadGenerator
{
    public interface IMessageClient
    {
        Task SendAsync(string payload);

        Task SendBatchAsync(List<string> payloads);

        Task CloseAsync();
    }

    public class MessageQueueClient : IMessageClient
    {
        QueueClient _queueClient = null;

        public MessageQueueClient(string connectionString, string eHOrQueueOrTopicName)
        {
            Console.WriteLine($"MessageClient | eHOrQueueOrTopicName:{eHOrQueueOrTopicName}");

            var cs = connectionString + ";TransportType=Amqp";
            var namespaceManager = NamespaceManager.CreateFromConnectionString(cs);
            if (!namespaceManager.QueueExists(eHOrQueueOrTopicName))
            {
                var queueDescription = new QueueDescription(eHOrQueueOrTopicName)
                {
                    EnablePartitioning = true,
                    MaxSizeInMegabytes = 1024
                };
                namespaceManager.CreateQueue(queueDescription);
            }

            var f = MessagingFactory.CreateFromConnectionString(connectionString + ";TransportType=Amqp" + ";OperationTimeout=00:00:02");
            _queueClient = f.CreateQueueClient(eHOrQueueOrTopicName);
        }

        public Task SendAsync(string payload)
        {
            var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(payload)))
            {
                ContentType = "application/json",
                Label = "MyPayload",
                TimeToLive = TimeSpan.FromMinutes(100)
            };
            return _queueClient.SendAsync(message);
        }

        public Task SendBatchAsync(List<string> payloads)
        {
            var messageBatchBrokeredMessage = new List<BrokeredMessage>();
            payloads.ForEach(payload => messageBatchBrokeredMessage.Add(new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(payload)))
            {
                ContentType = "application/json",
                Label = "MyPayload",
                TimeToLive = TimeSpan.FromMinutes(100)
            }));

            return _queueClient.SendBatchAsync(messageBatchBrokeredMessage);
        }

        public Task CloseAsync()
        {
            return _queueClient.CloseAsync();
        }
    }

    public class MessageEventHubClient : IMessageClient
    {
        EventHubClient _sendEventHubClient = null;

        public MessageEventHubClient(string connectionString, string eHOrQueueOrTopicName)
        {
            Console.WriteLine($"MessageClient | eHOrQueueOrTopicName:{eHOrQueueOrTopicName}");

            var mf = MessagingFactory.CreateFromConnectionString(connectionString + ";TransportType=Amqp" + ";OperationTimeout=00:00:02");
            _sendEventHubClient = mf.CreateEventHubClient(eHOrQueueOrTopicName);
        }

        public Task SendAsync(string payload)
        {
            return _sendEventHubClient.SendAsync(new EventData(new MemoryStream(Encoding.UTF8.GetBytes(payload))));
        }

        public Task SendBatchAsync(List<string> payloads)
        {
            var messageBatchEventHub = new List<EventData>();
            payloads.ForEach(payload => messageBatchEventHub.Add(new EventData(new MemoryStream(Encoding.UTF8.GetBytes(payload)))));
            return _sendEventHubClient.SendBatchAsync(messageBatchEventHub);
        }

        public Task CloseAsync()
        {
            return _sendEventHubClient.CloseAsync();
        }
    }

    public class MessageCloudQueueClient : IMessageClient
    {
        CloudQueue _cloudQueue = null;

        public MessageCloudQueueClient(string connectionString, string eHOrQueueOrTopicName)
        {
            Console.WriteLine($"MessageClient | eHOrQueueOrTopicName:{eHOrQueueOrTopicName}");

            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(eHOrQueueOrTopicName);
            queue.CreateIfNotExists();
            _cloudQueue = queue;
        }

        public Task SendAsync(string payload)
        {
            var options = new QueueRequestOptions() { ServerTimeout = TimeSpan.FromSeconds(1), MaximumExecutionTime = TimeSpan.FromSeconds(2) };
            return _cloudQueue.AddMessageAsync(new CloudQueueMessage(payload), null, null, options, null);
        }

        public Task SendBatchAsync(List<string> payloads)
        {
            var bigPayload = $"[{string.Join(",", payloads)}]";
            var joinedPayload = Encoding.UTF8.GetBytes(bigPayload);
            var messageBatchCloudQueueMessage = new CloudQueueMessage(joinedPayload);
            var options = new QueueRequestOptions() { ServerTimeout = TimeSpan.FromSeconds(1), MaximumExecutionTime = TimeSpan.FromSeconds(2) };
            return _cloudQueue.AddMessageAsync(messageBatchCloudQueueMessage, null, null, options, null);
        }

        public Task CloseAsync()
        {
            return Task.CompletedTask;
        }
    }

    public class MessageEventGridClient : IMessageClient
    {
        EventGridClient _sendEventGridClient = null;
        private string _topicHostname;
        /// <summary>
        /// Connection string is comma separated values. 
        /// Expected values are Endpoint, TopicKey
        /// Example "Endpoint=http://sfdsdfsdfsdfsdf,TopicKey=dfxxxcfxcfxf"
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="eHOrQueueOrTopicName"></param>
        public MessageEventGridClient(string connectionString, string eHOrQueueOrTopicName)
        {
            Console.WriteLine($"MessageClient | eHOrQueueOrTopicName:{eHOrQueueOrTopicName}");
            var keyValues = connectionString.Split(',');
            string topicKey = string.Empty;

            foreach (var keyValue in keyValues)
            {
                var kv = keyValue.Split('=');
                if (kv[0] == "Endpoint")
                {
                    _topicHostname = new Uri(kv[1]).Host;
                }
                if (kv[0] == "TopicKey")
                {
                    topicKey = kv[1] + "=";
                }
            }
            TopicCredentials topicCredentials = new TopicCredentials(topicKey);
            _sendEventGridClient = new EventGridClient(topicCredentials);
        }

        public Task SendAsync(string payload)
        {
            return _sendEventGridClient.PublishEventsAsync(
                _topicHostname, new List<EventGridEvent>() {
                    new EventGridEvent() {Id = Guid.NewGuid().ToString(),
                    EventType = "type1",
                    Data = payload,
                    EventTime = DateTime.Now,
                    Subject = "subject1",
                    DataVersion = "1.0" }});
        }

        public Task SendBatchAsync(List<string> payloads)
        {
            var events = new List<EventGridEvent>();
            foreach (var payload in payloads)
            {
                events.Add(new EventGridEvent()
                {
                    Id = Guid.NewGuid().ToString(),
                    EventType = "type1",
                    Data = payload,
                    EventTime = DateTime.Now,
                    Subject = "subject1",
                    DataVersion = "1.0"
                });
            }

            return _sendEventGridClient.PublishEventsAsync(_topicHostname, events);
        }

        public Task CloseAsync()
        {
            return Task.CompletedTask;
        }
    }
}
