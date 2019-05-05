namespace NonBlockingConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

    using Confluent.Kafka;

    using Newtonsoft.Json;

    using StackExchange.Redis;

    public static class SimpleConsumer
    {
        // whith this strategy we have fast message consumption (100ms for 10 msg with prints) but we may lose the tasks in the background
        public static void StartConsumerWithQueriesForOffsets(string topic, ConsumerConfig config, CancellationTokenSource cts, ConnectionMultiplexer redis)
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
                            Console.WriteLine($"Timetaken Simple Consumer: {sw.ElapsedMilliseconds} ms");
                            consumer.Close();
                            break;
                        }

                        Console.WriteLine($"Consumed: {msg.Message?.Value}");

                        // possible duplicate consumptin of messages if we assign manually
                        // if we assign before commit, the consumer will pick the message again, almost like a retry
                        //consumer.Assign(msg.TopicPartition);

                        var wmAfterAssign = consumer.GetWatermarkOffsets(msg.TopicPartition);

                        //here only one will be set, the high one
                        Console.WriteLine($"Before Commit -> Watermark Offsets: High -> {wmAfterAssign.High.Value} || Low -> {wmAfterAssign.Low.Value}");

                        var beforeCommitOffsets = consumer.Committed(new List<TopicPartition> { msg.TopicPartition }, TimeSpan.FromMinutes(1));
                        Console.WriteLine($"Before Commit -> Last Offset Committed: {beforeCommitOffsets.First()?.Offset}");

                        consumer.Commit(msg);
                        var committedOffsets = consumer.Committed(new List<TopicPartition> { msg.TopicPartition }, TimeSpan.FromMinutes(1));
                        Console.WriteLine($"After Commit -> Last Offset Committed: {committedOffsets.First()?.Offset}");
                        db.StringSet(LastOffSetKey, JsonConvert.SerializeObject(committedOffsets));

                        var wmAfterCommit = consumer.GetWatermarkOffsets(msg.TopicPartition);

                        // Here both will be set, Low and High (low should be 0, High should be total number o messages produced for example)
                        Console.WriteLine($"After Commit -> Watermark Offsets: High -> {wmAfterCommit.High.Value} || Low -> {wmAfterCommit.Low.Value}");
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