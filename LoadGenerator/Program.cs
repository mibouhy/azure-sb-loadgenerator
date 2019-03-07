using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

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

                ThreadPool.SetMinThreads(commandLineOptions.Threads, commandLineOptions.Threads);

                var sendClient = new SendMessageClient(commandLineOptions.ConnectionString, commandLineOptions.EHOrQueueOrTopicName, commandLineOptions.ClientType);

                var app = new Program();

                sendMessagesTasksDuration.Start();
                Task.WaitAll(app.SendMessagesTasks(commandLineOptions, sendClient).ToArray());
                sendMessagesTasksDuration.Stop();

                sendClient.CloseAsync().Wait();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.ToString());
                return 1;
            }
            Console.WriteLine($"Execution Completed. Send messages tasks duration: {sendMessagesTasksDuration.Elapsed}");
            return 0;
        }

        private List<Task> SendMessagesTasks(CommandLineOptionsClass commandLineOptions, SendMessageClient sendClient)
        {
            var tasks = new List<Task>();

            for (int threadId = 0; threadId < commandLineOptions.Threads; threadId++)
            {
                var t = GenerateLoadPerThread(commandLineOptions, threadId.ToString("0000"), sendClient);
                tasks.Add(t);
            }
            return tasks;
        }

        private async Task GenerateLoadPerThread(CommandLineOptionsClass commandLineOptions, string threadId, SendMessageClient sendClient)
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
                            PrintSpeed(threadId, commandLineOptions, messageNumber, sendingLag, messages.Count);
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
            Console.WriteLine($"Thread: {threadId} | sent: {messageNumber} / {commandLineOptions.MessagesToSend} messages total | BatchSize: {commandLineOptions.BatchSize} | " +
                $"potential speed: {(messagesCount / sendingLag.Elapsed.TotalSeconds).ToString("0.0")} msg/sec | " +
                $"speed: {(messagesCount / (sendingLag.Elapsed.TotalSeconds + (double)commandLineOptions.DelayPerThreadBetweenMessagesInMs / 1000)).ToString("0.0")} msg/sec | " +
                $"sendingLag: {(long)sendingLag.Elapsed.TotalMilliseconds} ms");
        }

        private void PrintException(string threadId, long messageNumber, Stopwatch sendingLag, long failedRequestsCount, long successfulRequestsCount, Exception e)
        {
            Console.Error.WriteLine($"Thread: {threadId} | messageNumber: {messageNumber} | " +
                $"{e.GetType().Name} | " +
                $"success: {successfulRequestsCount} | " +
                $"failures: {failedRequestsCount} | " +
                $"sendingLag: {(long)sendingLag.Elapsed.TotalMilliseconds} ms");
        }
    }
}
