using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQConsumer.Models;
using System.Text;
using System.Text.Json;

namespace RabbitMQConsumer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ConsumerController : ControllerBase
    {
        [HttpGet]
        public async Task<IActionResult> GetMessage()
        {
            var response = new List<Mensagem>();
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "minhafila1",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var msg = JsonSerializer.Deserialize<Mensagem>(message);

                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                        response.Add(msg);
                        Console.WriteLine("========================================================");
                        Console.WriteLine($"Mensagem id:{msg.Id}, descriçao:{msg.Descricao}, ");
                        Console.WriteLine("========================================================\n");
                    }
                    catch (Exception e)
                    {
                        channel.BasicNack(deliveryTag: ea.DeliveryTag, false, true);
                        throw e;
                    }
                };

                channel.BasicConsume(queue: "minhafila1",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();

                return Ok(response);
            }
        }
    }
}