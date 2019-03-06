using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace LoadGenerator
{
    public class SendMessageClient
    {
        QueueClient _queueClient = null;
        EventHubClient _eventHubClient = null;
        CloudQueue _cloudQueue = null;
        readonly ClientType _clientType = ClientType.Undefined;

        public SendMessageClient(string connectionString, string eHOrQueueOrTopicName, ClientType clientType)
        {
            Console.WriteLine($"SendMessageClient | eHOrQueueOrTopicName:{eHOrQueueOrTopicName} | clientType:{clientType.ToString()}");

            _clientType = clientType;

            if (ClientType.QueueClient == clientType)
            {
                _queueClient = QueueClient.CreateFromConnectionString(connectionString, eHOrQueueOrTopicName);
                return;
            }

            if (ClientType.EventHub == clientType)
            {
                var mf = MessagingFactory.CreateFromConnectionString(connectionString + ";TransportType=Amqp" + ";OperationTimeout=00:00:02");
                _eventHubClient = mf.CreateEventHubClient(eHOrQueueOrTopicName);
                return;
            }

            if (ClientType.CloudQueueClient == clientType)
            {
                var storageAccount = CloudStorageAccount.Parse(connectionString);
                var queueClient = storageAccount.CreateCloudQueueClient();
                var queue = queueClient.GetQueueReference(eHOrQueueOrTopicName);
                queue.CreateIfNotExists();
                _cloudQueue = queue;
                return;
            }

            throw new NotImplementedException();
        }

        public Task SendAsync(string payload)
        {
            if (ClientType.QueueClient == _clientType)
            {
                var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(payload)))
                {
                    ContentType = "application/json",
                    Label = "MyPayload",
                    TimeToLive = TimeSpan.FromMinutes(100)
                };
                return _queueClient.SendAsync(message);
            }

            if (ClientType.EventHub == _clientType)
            {
                var message = new EventData(new MemoryStream(Encoding.UTF8.GetBytes(payload)));
                return _eventHubClient.SendAsync(message);
            }

            if (ClientType.CloudQueueClient == _clientType)
            {
                CloudQueueMessage message = new CloudQueueMessage(payload);
                var options = new QueueRequestOptions() { ServerTimeout = TimeSpan.FromSeconds(1), MaximumExecutionTime = TimeSpan.FromSeconds(2) };
                return _cloudQueue.AddMessageAsync(message, null, null, options, null);
            }

            throw new NotImplementedException();
        }

        public Task SendBatchAsync(List<string> payloads)
        {
            if (ClientType.QueueClient == _clientType)
            {
                var messageBatch = new List<BrokeredMessage>();
                payloads.ForEach(payload => messageBatch.Add(new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(payload)))
                {
                    ContentType = "application/json",
                    Label = "MyPayload",
                    TimeToLive = TimeSpan.FromMinutes(100)
                }));

                return _queueClient.SendBatchAsync(messageBatch);
            }

            if (ClientType.EventHub == _clientType)
            {
                var aggregationLag = Stopwatch.StartNew();
                var messageBatch = new List<EventData>();
                payloads.ForEach(payload => messageBatch.Add(new EventData(new MemoryStream(Encoding.UTF8.GetBytes(payload)))));
                return _eventHubClient.SendBatchAsync(messageBatch);
            }

            if (ClientType.CloudQueueClient == _clientType)
            {
                var bigPayload = string.Join(",", payloads);
                var joinedPayload = Encoding.UTF8.GetBytes(bigPayload);
                var messageBatch = new CloudQueueMessage(joinedPayload);
                var options = new QueueRequestOptions() { ServerTimeout = TimeSpan.FromSeconds(1), MaximumExecutionTime = TimeSpan.FromSeconds(2) };

                return _cloudQueue.AddMessageAsync(messageBatch, null, null, options, null);
            }

            throw new NotImplementedException();
        }

        public Task CloseAsync()
        {
            if (ClientType.QueueClient == _clientType)
            {
                return _queueClient.CloseAsync();
            }

            if (ClientType.EventHub == _clientType)
            {
                return _eventHubClient.CloseAsync();
            }

            if (ClientType.CloudQueueClient == _clientType)
            {
                return Task.CompletedTask;
            }

            throw new NotImplementedException();
        }
    }
    public enum ClientType
    {
        Undefined = 0,
        EventHub = 1,
        QueueClient = 2,
        CloudQueueClient = 3
    }
}
