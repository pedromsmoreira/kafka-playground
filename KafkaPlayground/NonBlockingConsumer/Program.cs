namespace NonBlockingConsumer
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using Confluent.Kafka;

    using StackExchange.Redis;

    internal static class Program
    {
        // todo: split consumers and producer in different classes
        private static async Task Main(string[] args)
        {
            var redis = ConnectionMultiplexer.Connect("localhost");

            const string NonBlockingTopic = "non-block-topic";
            CancellationTokenSource cts = new CancellationTokenSource();
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer",
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

            await ProduceUntilCancelled(NonBlockingTopic, cts, producerConfig).ConfigureAwait(false);

            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            Console.WriteLine($"Start Query Consumer");

            SimpleConsumer.StartConsumerWithQueriesForOffsets(NonBlockingTopic, consumerConfig, cts, redis);

            // blocking consumer configs
            var blockingConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "blocking-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            Console.WriteLine($"Start Blocking Consumer");
            await SlowConsumer.StartBlockingConsumer(NonBlockingTopic, blockingConsumerConfig, cts, redis).ConfigureAwait(false);

            Console.WriteLine($"Start Non Blocking Consumer");
            var nonBlockingConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "non-blocking-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            TaskBasedMessageConsumer.StartNonBlockingConsumer(NonBlockingTopic, nonBlockingConsumerConfig, cts, redis);

            Console.WriteLine($"Start Dataflow Consumer");
            var dataflowConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "dataflow-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            await DataflowConsumer.StartDataflowConsumer(NonBlockingTopic, dataflowConsumerConfig, cts, redis).ConfigureAwait(false);
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
                        var report = await producer.ProduceAsync(topic, new Message<string, string> { Key = $"key-{i}", Value = $"{i}" }).ConfigureAwait(false);

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
    }
}