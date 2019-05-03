namespace NonBlockingConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Confluent.Kafka;

    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            const string NonBlockingTopic = "non-block-topic";
            CancellationTokenSource cts = new CancellationTokenSource();
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "non-blocking-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            // produce dummy data to dummy-topic
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
                StatisticsIntervalMs = 5000,
                ClientId = "playground-producer-01",
                EnableIdempotence = true
            };

            await ProduceUntilCancelled(NonBlockingTopic, cts, producerConfig);
            // start consumer

            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            await StartConsumer(NonBlockingTopic, consumerConfig, cts);
        }

        private static async Task ProduceUntilCancelled(string topic, CancellationTokenSource cts, ProducerConfig producerConfig)
        {
            var i = 0;
            do
            {
                using (var producer =
                    new ProducerBuilder<string, string>(producerConfig)
                        .SetStatisticsHandler((_, json) =>
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"Producer Statistics: {json}");
                            Console.ResetColor();
                        })
                        .Build())
                {
                    try
                    {
                        var report = await producer.ProduceAsync(topic, new Message<string, string> { Key = $"key-{i}", Value = $"{i}" });

                        Console.WriteLine($"delivered to: {report.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }
                i++;
            }
            while (i < 10);
        }

        private static async Task StartConsumer(string topic, ConsumerConfig config, CancellationTokenSource cts)
        {
            const int commitPeriod = 5;

            //var consumeTask = Task.Factory.StartNew(() =>
            //{
            using (var consumer =
                new ConsumerBuilder<Ignore, string>(config)
                    .SetStatisticsHandler((_, json) =>
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Consumer Statistics: {json}");
                        Console.ResetColor();
                    })
                    .Build())
            {
                consumer.Subscribe(topic);

                var queryWatermarks = consumer.GetWatermarkOffsets(new TopicPartition(topic, new Partition(0)));

                // here they are unset  (High: -1001, Low: -1001)
                Console.WriteLine($"Before Consumer and assigning offsets Watermark Offsets: High -> {queryWatermarks.High.Value} || Low -> {queryWatermarks.Low.Value}");
                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var msg = consumer.Consume(cts.Token);

                        if (msg == null || msg.IsPartitionEOF || msg.Message == null)
                        {
                            Console.WriteLine($"Reached end of topic {msg.Topic}, partition {msg.Partition}, offset {msg.Offset}.");

                            continue;
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

                        var wmAfterCommit = consumer.GetWatermarkOffsets(msg.TopicPartition);

                        // Here both will be set, Low and High (low should be 0, High should be total number o messages produced for example)
                        Console.WriteLine($"After Commit -> Watermark Offsets: High -> {wmAfterCommit.High.Value} || Low -> {wmAfterCommit.Low.Value}");
                    }
                }
                catch (Exception)
                {
                    throw;
                }

                //consumer.Consume();

                // store offsets in db (maybe redis for simplicity sake)

                // use TPL Dataflow or RxExtensions to create non-blocking consumer
            }
            //}, TaskCreationOptions.LongRunning);
        }
    }
}