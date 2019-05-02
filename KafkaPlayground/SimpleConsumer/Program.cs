namespace SimpleConsumer
{
    using Confluent.Kafka;

    using System;
    using System.Threading;
    using System.Threading.Tasks;

    internal static class Program
    {
        private const string SimpleTopic = "simple-topic";

        private static async Task Main(string[] args)
        {
            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "simple-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            const int commitPeriod = 5;

            var consumeTask = Task.Run(() =>
            {
                using (var consumer =
                    new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetStatisticsHandler((_, json) =>
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"Consumer Statistics: {json}");
                            Console.ResetColor();

                        })
                        .Build())
                {
                    consumer.Subscribe(SimpleTopic);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cts.Token);

                                if (consumeResult.IsPartitionEOF)
                                {
                                    Console.WriteLine(
                                        $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                    continue;
                                }

                                Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

                                if (consumeResult.Offset % commitPeriod == 0)
                                {
                                    // The Commit method sends a "commit offsets" request to the Kafka
                                    // cluster and synchronously waits for the response. This is very
                                    // slow compared to the rate at which the consumer is capable of
                                    // consuming messages. A high performance application will typically
                                    // commit offsets relatively infrequently and be designed handle
                                    // duplicate messages in the event of failure.
                                    try
                                    {
                                        consumer.Commit(consumeResult);
                                    }
                                    catch (KafkaException e)
                                    {
                                        Console.WriteLine($"Commit error: {e.Error.Reason}");
                                    }
                                }
                            }
                            catch (ConsumeException ce)
                            {
                                Console.WriteLine($"Consume error: {ce.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Closing consumer.");
                        consumer.Close();
                    }
                }
            });

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.Leader,
                StatisticsIntervalMs = 5000,
                ClientId = "playground-producer-01",
            };

            var i = 0;
            while (!cts.IsCancellationRequested)
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
                        var report = await producer.ProduceAsync(SimpleTopic, new Message<string, string> { Key = $"key-{i}", Value = $"{i}" });

                        Console.WriteLine($"delivered to: {report.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }
                i++;
            }
        }
    }
}