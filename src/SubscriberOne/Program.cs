using System;
using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace SubscriberOne
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;

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

            var subName = "reciever-" + Guid.NewGuid();
            var subDetails = new SubscriptionDescription(topic, subName)
            {
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5),

            };

            NamespaceManager.Create().CreateSubscription(subDetails, new SqlFilter("universe = 'marvel'"));


            var reciever = recieverFactory.CreateSubscriptionClient(topic, subName, ReceiveMode.PeekLock);


            reciever.OnMessage(message =>
            {
                Console.Write($"Message received from Topic{topic}:");

                dynamic content = JsonConvert.DeserializeObject(message.GetBody<string>());
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
                message.Complete();

            },
            new OnMessageOptions
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false,
            });



            Console.WriteLine("Reciever Listening...");
            Console.ReadLine();
        }
    }
}
