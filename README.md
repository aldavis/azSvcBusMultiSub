# commando
Simple example of using Topics and Subscriptions with Azure Service Bus

## Getting Started
* Create an azure service bus namespace (standard tier required).
* Create 2 Shared Access Signatures for the namespace, 1 for the Sender and 1 for the Reciever.
* Create a privateSettings.config file in the root of the src folder which holds the connection string to your service bus namespace as well as the keys and names of the Shared Access Signatures.

Example:
```
<?xml version="1.0" encoding="utf-8"?>
<appSettings>
    <!-- Service Bus specific app setings for messaging connections -->
    <add key="Microsoft.ServiceBus.ConnectionString"
        value="Endpoint=sb://[your namespace].servicebus.windows.net;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[your secret]"/>
  <add key="ServiceBusNamespace" value="sb://[your namespace].servicebus.windows.net/"/>
  <add key="SenderKey" value="[sender key]"/>
  <add key="SenderKeyName" value="[key name]"/>
  <add key="RecieverKey" value="[reciever key]"/>
  <add key="RecieverKeyName" value="[reciever name]"/>
</appSettings>
```


