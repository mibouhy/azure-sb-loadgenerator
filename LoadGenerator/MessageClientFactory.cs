using System;

namespace LoadGenerator
{
    public static class MessageClientFactory
    {
        public static IClientSender CreateMessageClient(string connectionString, string eHOrQueueOrTopicName, ClientType clientType)
        {
            switch (clientType)
            {
                case ClientType.CloudQueueClient:
                    return new MessageCloudQueueClient(connectionString, eHOrQueueOrTopicName);
                case ClientType.EventHub:
                    return new MessageEventHubClient(connectionString, eHOrQueueOrTopicName);
                case ClientType.QueueClient:
                    return new MessageQueueClient(connectionString, eHOrQueueOrTopicName);
                case ClientType.EventGridClient:
                    return new EventGridClientSender(connectionString, eHOrQueueOrTopicName);
                case ClientType.Undefined:
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
        CloudQueueClient = 3,
        EventGridClient = 4
    }
}
