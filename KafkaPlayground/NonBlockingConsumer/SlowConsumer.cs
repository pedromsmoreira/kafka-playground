namespace NonBlockingConsumer
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    using Confluent.Kafka;
    using Newtonsoft.Json;
    using StackExchange.Redis;

    public static class SlowConsumer
    {
        // We this strategy we have slow consumption of messages and don't lose content or actions
        public static async Task StartBlockingConsumer(string topic, ConsumerConfig config, CancellationTokenSource cts, ConnectionMultiplexer redis)
        {
            var db = redis.GetDatabase();
            const string LastOffSetKey = "last-comitted-offset";
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

                        if (msg == null || msg.IsPartitionEOF || msg.Message == null)
                        {
                            Console.WriteLine($"Reached end of topic {msg.Topic}, partition {msg.Partition}, offset {msg.Offset}.");
                            sw.Stop();
                            Console.WriteLine($"Timetaken Blocking Consumer: {sw.ElapsedMilliseconds} ms");
                            consumer.Close();

                            break;
                        }

                        Console.WriteLine($"Blocking Consumer -> Consumed: {msg.Message?.Value}");
                        await Task.Delay(TimeSpan.FromMilliseconds(500));
                        consumer.Commit(msg);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Consumer will close.");
                    consumer.Close();
                }

                var offsets = db.StringGet(LastOffSetKey);
                Console.WriteLine($"Serialized Object -> {JsonConvert.SerializeObject(offsets)}");
            }
        }
    }
}