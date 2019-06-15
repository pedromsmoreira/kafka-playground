namespace NonBlockingConsumer
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using Confluent.Kafka;

    using StackExchange.Redis;

    public static class DataflowConsumer
    {
        // with this configurations we take 1500ms to consume 10 msg with a delay of 500ms
        public static async Task StartDataflowConsumer(string topic, ConsumerConfig config, CancellationTokenSource cts, ConnectionMultiplexer redis)
        {
            var printBlock = new ActionBlock<string>(
                msg =>
                {
                    Console.WriteLine($"Dataflow Consumer -> Consumed: {msg}");
                    Task.Delay(TimeSpan.FromMilliseconds(500)).Wait();
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 10,
                    BoundedCapacity = 100
                }
                );
            var db = redis.GetDatabase();
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(topic);

                var sw = new Stopwatch();
                var swBlock = new Stopwatch();
                var hasStarted = false;
                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var msg = consumer.Consume(cts.Token);

                        if (!hasStarted)
                        {
                            sw.Start();
                            swBlock.Start();
                            hasStarted = true;
                        }

                        if (msg?.IsPartitionEOF != false || msg.Message == null)
                        {
                            Console.WriteLine($"Reached end of topic {msg.Topic}, partition {msg.Partition}, offset {msg.Offset}.");
                            sw.Stop();
                            Console.WriteLine($"Timetaken Dataflow Consumer: {sw.ElapsedMilliseconds} ms");
                            consumer.Close();
                            printBlock.Complete();

                            await printBlock.Completion;
                            swBlock.Stop();
                            Console.WriteLine($"Timetaken to finish every process: {swBlock.ElapsedMilliseconds} ms");
                            break;
                        }

                        var r = await printBlock.SendAsync<string>(msg.Message.Value).ConfigureAwait(false);

                        consumer.Commit(msg);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Dataflow Consumer will close.");
                    consumer.Close();
                    printBlock.Complete();
                    await printBlock.Completion;
                }

                //await printBlock.Completion;
                //swBlock.Stop();
                //Console.WriteLine($"Timetaken to finish every process: {swBlock.ElapsedMilliseconds} ms");
            }
        }
    }
}