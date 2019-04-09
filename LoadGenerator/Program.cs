using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs.Processor;

namespace LoadGenerator
{
    class Program
    {
        public static int Main(string[] args)
        {
            var sendMessagesTasksDuration = new Stopwatch();
            try
            {
                CommandLineOptionsClass commandLineOptions = new CommandLineOptionsClass();
                var isValid = CommandLine.Parser.Default.ParseArgumentsStrict(args, commandLineOptions);

                if (commandLineOptions.ClientType == ClientType.EventHub && !string.IsNullOrEmpty(commandLineOptions.StorageAccountConnectionString))
                {
                    //read from event hub
                    var processorHost = new EventProcessorHost(commandLineOptions.EHOrQueueOrTopicName, "consumerx",
                        commandLineOptions.ConnectionString, commandLineOptions.StorageAccountConnectionString,
                        "event-lease-container");
                    processorHost.RegisterEventProcessorAsync<SimpleEventProcessor>().Wait();
                    Console.WriteLine("Receiving. Press enter key to stop worker.");
                    Console.ReadLine();

                    // Disposes of the Event Processor Host
                    processorHost.UnregisterEventProcessorAsync().Wait();
                }
                else
                {
                    //otherwise write logic

                    ThreadPool.SetMinThreads(commandLineOptions.Threads, commandLineOptions.Threads);

                    var sendClient = MessageClientFactory.CreateMessageClient(commandLineOptions.ConnectionString, commandLineOptions.EHOrQueueOrTopicName, commandLineOptions.ClientType);

                    var app = new Program();

                    sendMessagesTasksDuration.Start();
                    Task.WaitAll(app.SendMessagesTasks(commandLineOptions, sendClient).ToArray());
                    sendMessagesTasksDuration.Stop();

                    sendClient.CloseAsync().Wait();
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.ToString());
                return 1;
            }
            Console.WriteLine($"Execution Completed. Duration: {sendMessagesTasksDuration.Elapsed}");
            return 0;
        }

        private List<Task> SendMessagesTasks(CommandLineOptionsClass commandLineOptions, IMessageClient sendClient)
        {
            var tasks = new List<Task>();

            for (int threadId = 0; threadId < commandLineOptions.Threads; threadId++)
            {
                var t = GenerateLoadPerThread(commandLineOptions, threadId.ToString("0000"), sendClient);
                tasks.Add(t);
            }
            return tasks;
        }

        private async Task GenerateLoadPerThread(CommandLineOptionsClass commandLineOptions, string threadId, IMessageClient sendClient)
        {
            string now;
            string randomPayload;
            string payload;
            var sendingLag = Stopwatch.StartNew();
            long messageNumber = 0;
            long failedRequestsCount = 0;
            long successfulRequestsCount = 0;

            Console.WriteLine($"Thread: {threadId}, started and connected | BatchMode: {commandLineOptions.BatchMode}");
            var messages = new List<string>();

            while (messageNumber < commandLineOptions.MessagesToSend)
            {
                messageNumber++;
                now = DateTime.UtcNow.Ticks.ToString();
                randomPayload = new Bogus.Randomizer().ClampString("", commandLineOptions.MessageSize, commandLineOptions.MessageSize);
                payload = string.Format("{{\"dt\":{0},\"payload\":\"{1}\"}}", now, randomPayload);

                if (commandLineOptions.BatchMode.ToLower() == "false")
                {
                    try
                    {
                        sendingLag.Restart();
                        await sendClient.SendAsync(payload).ConfigureAwait(false);
                        successfulRequestsCount++;
                        if (messageNumber % commandLineOptions.Checkpoint == 0 && messageNumber > 0)
                        {
                            PrintSpeed(threadId, commandLineOptions, messageNumber, sendingLag, 1);
                            if (messageNumber < commandLineOptions.MessagesToSend && commandLineOptions.DelayPerThreadBetweenMessagesInMs > 0)
                            {
                                await Task.Delay(TimeSpan.FromMilliseconds(commandLineOptions.DelayPerThreadBetweenMessagesInMs)).ConfigureAwait(false);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        failedRequestsCount++;
                        PrintException(threadId, messageNumber, sendingLag, failedRequestsCount, successfulRequestsCount, e);
                        await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                    }
                }
                else
                {
                    if (messages.Count < commandLineOptions.BatchSize)
                    {
                        messages.Add(payload);
                    }
                    if (messages.Count >= commandLineOptions.BatchSize || messageNumber >= commandLineOptions.MessagesToSend)
                    {
                        try
                        {
                            sendingLag.Restart();
                            await sendClient.SendBatchAsync(messages).ConfigureAwait(false);
                            successfulRequestsCount++;
                            PrintSpeed(threadId, commandLineOptions, messageNumber, sendingLag, messages.Count);
                            messages.Clear();

                            if (messageNumber < commandLineOptions.MessagesToSend && commandLineOptions.DelayPerThreadBetweenMessagesInMs > 0)
                            {
                                await Task.Delay(TimeSpan.FromMilliseconds(commandLineOptions.DelayPerThreadBetweenMessagesInMs)).ConfigureAwait(false);
                            }
                        }
                        catch (Exception e)
                        {
                            failedRequestsCount++;
                            PrintException(threadId, messageNumber, sendingLag, failedRequestsCount, successfulRequestsCount, e);
                            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                        }
                    }
                }
            }
            Console.WriteLine($"Thread: {threadId}, finished | successfulRequestsCount: {successfulRequestsCount} | failedRequestsCount: {failedRequestsCount}");
        }

        private void PrintSpeed(string threadId, CommandLineOptionsClass commandLineOptions, long messageNumber, Stopwatch sendingLag, long messagesCount)
        {
            var message = $"Thread: {threadId} | sent: {messageNumber} / {commandLineOptions.MessagesToSend} messages total | " +
                $"sendingLag: {(long)sendingLag.Elapsed.TotalMilliseconds} ms | " +
                $"speed: {(messagesCount / (sendingLag.Elapsed.TotalSeconds + (double)commandLineOptions.DelayPerThreadBetweenMessagesInMs / 1000)).ToString("0.0")} msg/sec";
            if (commandLineOptions.DelayPerThreadBetweenMessagesInMs > 0)
            {
                message += $" | potential speed: {(messagesCount / sendingLag.Elapsed.TotalSeconds).ToString("0.0")} msg/sec";
            }
            Console.WriteLine(message);
        }

        private void PrintException(string threadId, long messageNumber, Stopwatch sendingLag, long failedRequestsCount, long successfulRequestsCount, Exception e)
        {
            Console.Error.WriteLine($"Thread: {threadId} | messageNumber: {messageNumber} | " +
                $"{e.GetType().Name} | " +
                $"{e.Message} | " +
                $"success: {successfulRequestsCount} | " +
                $"failures: {failedRequestsCount} | " +
                $"sendingLag: {(long)sendingLag.Elapsed.TotalMilliseconds} ms");
        }
    }
}
