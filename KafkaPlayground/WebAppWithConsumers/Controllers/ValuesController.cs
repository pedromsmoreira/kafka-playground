namespace WebAppWithConsumers.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using Confluent.Kafka;

    using Microsoft.AspNetCore.Mvc;

    using MultipleConsumer;

    [Route("api/messages")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post()
        {
            // produce dummy data to dummy-topic
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
                StatisticsIntervalMs = 5000,
                ClientId = "playground-producer-01",
                EnableIdempotence = true
            };

            using (var producer =
                        new ProducerBuilder<string, ProductEvent>(producerConfig)
                            .SetKeySerializer(new StringSerializer())
                            .SetValueSerializer(new ProductSerializer())
                            .Build())
            {
                try
                {
                    var productCreatedEvent = new ProductCreatedEvent
                    {
                        Id = "1",
                        DateCreated = DateTime.UtcNow,
                        Name = "product created",
                        Stock = 1
                    };

                    var productDeletedEvent = new ProductDeletedEvent
                    {
                        Id = "1",
                        DateDeleted = DateTime.UtcNow
                    };

                    var productStockIncreasedEvent = new ProductStockIncreasedEvent
                    {
                        Id = "1",
                        Quantity = 1
                    };

                    var productStockDecreasedEvent = new ProductStockDecreasedEvent
                    {
                        Id = "1",
                        Quantity = 2
                    };

                    var report = await producer.ProduceAsync("product-events-1.0", new Message<string, ProductEvent> { Key = productCreatedEvent.Id, Value = productCreatedEvent }).ConfigureAwait(false);
                    var report2 = await producer.ProduceAsync("product-events-1.0", new Message<string, ProductEvent> { Key = productDeletedEvent.Id, Value = productDeletedEvent }).ConfigureAwait(false);
                    var report3 = await producer.ProduceAsync("product-events-1.0", new Message<string, ProductEvent> { Key = productStockIncreasedEvent.Id, Value = productStockIncreasedEvent }).ConfigureAwait(false);
                    var report4 = await producer.ProduceAsync("product-events-1.0", new Message<string, ProductEvent> { Key = productStockDecreasedEvent.Id, Value = productStockDecreasedEvent }).ConfigureAwait(false);

                    return this.Ok(
                        new List<DeliveryResult<string, ProductEvent>>
                        {
                            report, report2, report3, report4
                        });
                }
                catch (ProduceException<string, string> e)
                {
                    this.BadRequest(e);
                }
            }

            return this.BadRequest("something went wrong");
        }
    }
}