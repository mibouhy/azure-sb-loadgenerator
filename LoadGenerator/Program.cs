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
                            Console.WriteLine($"Thread: {threadId} | sent: {messageNumber} / {commandLineOptions.MessagesToSend} messages | " +
                                $"speed: {messageNumber / sendingLag.Elapsed.TotalSeconds} msg/sec | sendingLag: {sendingLag.Elapsed}");
                        }
                    }
                    catch (Exception e)
                    {
                        failedRequestsCount++;
                        Console.Error.WriteLine($"Thread: {threadId} | Failed to SendAsync | " +
                            $"messageNumber: {messageNumber} because of {e.GetType().Name} | successfulRequestsCount: {successfulRequestsCount} | " +
                            $"failedRequestsCount: {failedRequestsCount} | sendingLag: {sendingLag.Elapsed}");
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
                            Console.WriteLine($"Thread: {threadId} | sent: {messageNumber} / {commandLineOptions.MessagesToSend} messages total | BatchSize: {commandLineOptions.BatchSize} " +
                                $"| speed: {messageNumber / sendingLag.Elapsed.TotalSeconds} msg/sec | sendingLag: {sendingLag.Elapsed}");
                            messages.Clear();
                        }
                        catch (Exception e)
                        {
                            failedRequestsCount++;
                            Console.Error.WriteLine($"Thread: {threadId} | Failed to SendBatchAsync | " +
                                $"messageNumber: {messageNumber} because of {e.GetType().Name} | successfulRequestsCount: {successfulRequestsCount} | " +
                                $"failedRequestsCount: {failedRequestsCount} | messages.Count: {messages.Count} | sendingLag: {sendingLag.Elapsed}");
                            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                        }
                    }
                }
            }
            Console.WriteLine($"Thread: {threadId}, finished | successfulRequestsCount: {successfulRequestsCount} | failedRequestsCount: {failedRequestsCount}");
        }
    }
}
