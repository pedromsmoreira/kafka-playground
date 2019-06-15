namespace MultipleConsumer
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Confluent.Kafka;

    using Microsoft.Extensions.Hosting;

    using Newtonsoft.Json;

    public class ProductEventsConsumer : IHostedService
    {
        private readonly ConsumerBuilder<string, ProductEvent> consumerBuilder;

        private readonly CancellationTokenSource cts;

        public ProductEventsConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "multiple-events-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            this.consumerBuilder = new ConsumerBuilder<string, ProductEvent>(config)
                .SetKeyDeserializer(new StringSerializer())
                .SetValueDeserializer(new ProductSerializer());

            this.cts = new CancellationTokenSource();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            using (var consumer = this.consumerBuilder.Build())
            {
                consumer.Subscribe("product-events-1.0");

                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var msg = consumer.Consume(cts.Token);

                        consumer.Commit(msg);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Dataflow Consumer will close.");
                    consumer.Close();
                }
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            this.cts.Cancel();
            return Task.CompletedTask;
        }
    }

    public class StringSerializer : IDeserializer<string>, ISerializer<string>
    {
        private readonly Encoding encoder = Encoding.UTF8;

        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return this.encoder.GetString(data.ToArray());
        }

        public byte[] Serialize(string data, SerializationContext context)
        {
            return this.encoder.GetBytes(data);
        }
    }

    public class ProductSerializer : IDeserializer<ProductEvent>, ISerializer<ProductEvent>
    {
        private readonly Encoding encoder = Encoding.UTF8;

        private readonly JsonSerializerSettings settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        public ProductEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var val = this.encoder.GetString(data.ToArray());

            var type = Type.GetType(val);

            var typedObj = JsonConvert.DeserializeObject<ProductEvent>(val, this.settings);

            return typedObj;
        }

        public byte[] Serialize(ProductEvent data, SerializationContext context)
        {
            var serializedEvent = JsonConvert.SerializeObject(data);

            return this.encoder.GetBytes(serializedEvent);
        }
    }

    public class ProductEvent
    {
        public string Id { get; set; }
    }

    public class ProductCreatedEvent : ProductEvent
    {
        public string Name { get; set; }

        public int Stock { get; set; }

        public DateTime DateCreated { get; set; }
    }

    public class ProductDeletedEvent : ProductEvent
    {
        public DateTime DateDeleted { get; set; }
    }

    public class ProductStockIncreasedEvent : ProductEvent
    {
        public int Quantity { get; set; }
    }

    public class ProductStockDecreasedEvent : ProductEvent
    {
        public int Quantity { get; set; }
    }
}