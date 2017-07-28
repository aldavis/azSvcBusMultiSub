using System;
using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace Sender
{
    internal class Program
    {
        private static void Main()
        {
            Console.ForegroundColor = ConsoleColor.Green;

            var namespaceManager = NamespaceManager.Create();

            Console.WriteLine("Enter topic to send to");
            var topicName = Console.ReadLine();

            if (!namespaceManager.QueueExists(topicName))
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"{topicName} does not exist, creating it.");
                namespaceManager.CreateTopic(topicName);
                Console.WriteLine("Done");
                Console.ForegroundColor = ConsoleColor.Green;
            }

            var serviceBusNamespace = ConfigurationManager.AppSettings["ServiceBusNamespace"];
            var name = ConfigurationManager.AppSettings["senderKeyName"];
            var key = ConfigurationManager.AppSettings["SenderKey"];

            var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(name, key);


            Console.WriteLine("Creating Sender");

            var senderFactory = MessagingFactory.Create(
                new Uri(serviceBusNamespace),
                new MessagingFactorySettings
                {
                    TransportType = TransportType.Amqp,
                    TokenProvider = tokenProvider
                });

            var sender = senderFactory.CreateMessageSender(topicName);

            //specifies if and how often the message should retry to be sent if it fails
            sender.RetryPolicy = new RetryExponential(TimeSpan.Zero, TimeSpan.FromSeconds(5), 10);

            Console.WriteLine("Done, hit enter to send the messages.");

            Console.ReadLine();

            dynamic data = new[]
            {
                new {name = "Banner", firstName = "Bruce", universe = "marvel"},
                new {name = "Stark", firstName = "Tony", universe = "marvel"},
                new {name = "Rogers", firstName = "Steven", universe = "marvel"},
                new {name = "Parker", firstName = "Peter", universe = "marvel"},
                new {name = "Wayne", firstName = "Bruce", universe = "dc"},
                new {name = "Allen", firstName = "Barry", universe = "dc"},
                new {name = "Kent", firstName = "Clark", universe = "dc"},
                new {name = "Lane", firstName = "Lois", universe = "dc"},
                new {name = "Stacey", firstName = "Gwen", universe = "marvel"}
            };


            for (var i = 0; i < data.Length; i++)
            {
                var message = new BrokeredMessage(JsonConvert.SerializeObject(data[i]))
                {
                    ContentType = "application/json",
                    Label = "mylabel",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                message.Properties.Add("universe", data[i].universe);

                try
                {
                    sender.Send(message);
                }
                catch (MessagingException exception)
                {
                    Console.WriteLine(exception);
                }


                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                    Console.ResetColor();
                }
            }
            sender.Close();
        }
    }
}