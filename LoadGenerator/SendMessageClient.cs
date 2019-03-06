using System;
using System.Collections.Generic;
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

            switch (_clientType)
            {
                case ClientType.QueueClient:
                    _queueClient = QueueClient.CreateFromConnectionString(connectionString, eHOrQueueOrTopicName);
                    return;
                case ClientType.EventHub:
                    var mf = MessagingFactory.CreateFromConnectionString(connectionString + ";TransportType=Amqp" + ";OperationTimeout=00:00:02");
                    _eventHubClient = mf.CreateEventHubClient(eHOrQueueOrTopicName);
                    return;
                case ClientType.CloudQueueClient:
                    var storageAccount = CloudStorageAccount.Parse(connectionString);
                    var queueClient = storageAccount.CreateCloudQueueClient();
                    var queue = queueClient.GetQueueReference(eHOrQueueOrTopicName);
                    queue.CreateIfNotExists();
                    _cloudQueue = queue;
                    return;
                default:
                    throw new NotImplementedException();
            }
        }

        public Task SendAsync(string payload)
        {
            switch (_clientType)
            {
                case ClientType.QueueClient:
                    var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(payload)))
                    {
                        ContentType = "application/json",
                        Label = "MyPayload",
                        TimeToLive = TimeSpan.FromMinutes(100)
                    };
                    return _queueClient.SendAsync(message);
                case ClientType.EventHub:
                    return _eventHubClient.SendAsync(new EventData(new MemoryStream(Encoding.UTF8.GetBytes(payload))));
                case ClientType.CloudQueueClient:
                    var options = new QueueRequestOptions() { ServerTimeout = TimeSpan.FromSeconds(1), MaximumExecutionTime = TimeSpan.FromSeconds(2) };
                    return _cloudQueue.AddMessageAsync(new CloudQueueMessage(payload), null, null, options, null);
                default:
                    throw new NotImplementedException();
            }
        }

        public Task SendBatchAsync(List<string> payloads)
        {
            switch (_clientType)
            {
                case ClientType.QueueClient:
                    var messageBatchBrokeredMessage = new List<BrokeredMessage>();
                    payloads.ForEach(payload => messageBatchBrokeredMessage.Add(new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(payload)))
                    {
                        ContentType = "application/json",
                        Label = "MyPayload",
                        TimeToLive = TimeSpan.FromMinutes(100)
                    }));

                    return _queueClient.SendBatchAsync(messageBatchBrokeredMessage);
                case ClientType.EventHub:
                    var messageBatchEventHub = new List<EventData>();
                    payloads.ForEach(payload => messageBatchEventHub.Add(new EventData(new MemoryStream(Encoding.UTF8.GetBytes(payload)))));
                    return _eventHubClient.SendBatchAsync(messageBatchEventHub);
                case ClientType.CloudQueueClient:
                    var bigPayload = $"[{string.Join(",", payloads)}]";
                    var joinedPayload = Encoding.UTF8.GetBytes(bigPayload);
                    var messageBatchCloudQueueMessage = new CloudQueueMessage(joinedPayload);
                    var options = new QueueRequestOptions() { ServerTimeout = TimeSpan.FromSeconds(1), MaximumExecutionTime = TimeSpan.FromSeconds(2) };
                    return _cloudQueue.AddMessageAsync(messageBatchCloudQueueMessage, null, null, options, null);
                default:
                    throw new NotImplementedException();
            }
        }

        public Task CloseAsync()
        {
            switch (_clientType)
            {
                case ClientType.QueueClient:
                    return _queueClient.CloseAsync();
                case ClientType.EventHub:
                    return _eventHubClient.CloseAsync();
                case ClientType.CloudQueueClient:
                    return Task.CompletedTask;
                default:
                    throw new NotImplementedException();
            }
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
