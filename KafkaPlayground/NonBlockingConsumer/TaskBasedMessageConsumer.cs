namespace NonBlockingConsumer
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;

    using StackExchange.Redis;

    public static class TaskBasedMessageConsumer
    {
        // whith this strategy we have fast message consumption (100ms for 10 msg with prints) but we may lose the tasks in the background
        public static void StartNonBlockingConsumer(string topic, ConsumerConfig config, CancellationTokenSource cts, ConnectionMultiplexer redis)
        {
            var db = redis.GetDatabase();
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(topic);

                // on the first get it won't have any offsets stored since it gets the last cached offsets
                var queryWatermarks = consumer.GetWatermarkOffsets(new TopicPartition(topic, new Partition(0)));

                // here they are unset or they aren't cached  (High: -1001, Low: -1001)
                Console.WriteLine($"Before Consumer and assigning offsets Watermark Offsets: High -> {queryWatermarks.High.Value} || Low -> {queryWatermarks.Low.Value}");
                var sw = new Stopwatch();
                var hasStarted = false;
                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var msg = consumer.Consume(cts.Token);

                        if (!hasStarted)
                        {
                            sw.Start();
                            hasStarted = true;
                        }

                        if (msg?.IsPartitionEOF != false || msg.Message == null)
                        {
                            Console.WriteLine($"Reached end of topic {msg.Topic}, partition {msg.Partition}, offset {msg.Offset}.");
                            sw.Stop();
                            Console.WriteLine($"Timetaken Non Blocking Consumer: {sw.ElapsedMilliseconds} ms");
                            consumer.Close();
                            break;
                        }

                        Task.Run(
                            () =>
                            {
                                Console.WriteLine($"Non Blocking Consumer -> Consumed: {msg.Message?.Value}");
                                Task.Delay(TimeSpan.FromMilliseconds(500)).Wait();
                            });
                        consumer.Commit(msg);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Consumer will close.");
                    consumer.Close();
                }
            }
        }
    }
}