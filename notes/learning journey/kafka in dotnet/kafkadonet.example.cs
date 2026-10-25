using Confluent.Kafka;
using System;
using System.Threading;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,

    EnableAutoCommit = false, //turn off auto commit

    MaxPollIntervalMs = 300000, //5 minutes 
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsRevokedHandler((c, p) =>
    {
        Console.WriteLine($"Partitions revoked: {string.Join(", ", p)}");

        try
        {
            c.commit(); //commit before losing partitions
            Console.WriteLine("Committed offsets before revocation");

        }
        catch
        {
            Console.WriteLine("Failed to commit offsets before revocation");
        }
    })
    .SetPartitionsAssignedHandler((c, p) =>
    {
        Console.WriteLine($"Partitions assigned: {string.Join(", ", p)}");
    })
    .Build();

consumer.Subscribe("test-topic");

Console.CancelKeyPress += (_, e) => {
    e.Cancel = true;
    cts.Cancel();
};

try 
{
    while (!cts.Token.IsCancellationRequested)
    {
        try
        {
            var consumeResult = consumer.Consume(cts.Token);
            if (consumeResult.IsPartitionEO)
        }
    }
}