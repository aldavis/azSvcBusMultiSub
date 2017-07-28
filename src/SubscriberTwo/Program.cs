using System;
using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace SubscriberTwo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Magenta;

            Console.WriteLine("Enter the topic to subscribe to");
            var topic = Console.ReadLine();

            var serviceBusNamespace = ConfigurationManager.AppSettings["ServiceBusNamespace"];
            var name = ConfigurationManager.AppSettings["RecieverKeyName"];
            var key = ConfigurationManager.AppSettings["RecieverKey"];


            TokenProvider tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(name, key);


            Console.WriteLine("Creating Subscription");

            var recieverFactory = MessagingFactory.Create(
                new Uri(serviceBusNamespace),
                new MessagingFactorySettings
                {
                    TransportType = TransportType.Amqp,
                    TokenProvider = tokenProvider

                });

            var subName = "reciever-" + Guid.NewGuid().ToString();
            var subDetails = new SubscriptionDescription(topic, subName)
            {
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5)
            };

            var ruleDescription = new RuleDescription("changeUniverseName", new SqlFilter("universe = 'dc'"))
            {
                Action = new SqlRuleAction("SET universe = 'detective comics'")

            };

            NamespaceManager.Create().CreateSubscription(subDetails, ruleDescription);


            var reciever = recieverFactory.CreateSubscriptionClient(topic, subName, ReceiveMode.PeekLock);

            reciever.OnMessage(message =>
            {
                Console.Write($"Message received from Topic{topic}:");
                dynamic content = JsonConvert.DeserializeObject(message.GetBody<string>());

                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.WriteLine(
                    $@"Message received: MessageId = {message.MessageId}, 
                    SequenceNumber = {message.SequenceNumber}, 
                    EnqueuedTimeUtc = {message.EnqueuedTimeUtc},
                    ExpiresAtUtc = {message.ExpiresAtUtc}, 
                    ContentType = {message.ContentType}, 
                    Size = {message.Size},  
                    Content: [ firstname = {content.firstName}, name = {content.name}, universe = {content.universe} ]");

                foreach (var item in message.Properties)
                {
                    Console.Write(" {0}={1}", item.Key, item.Value);
                }
                Console.WriteLine();
            });



            Console.WriteLine("Reciever Listening...");
            Console.ReadLine();
        }
    }
}
