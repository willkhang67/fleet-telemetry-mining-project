using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Ingest.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly KafkaOptions _options;

    public Worker(ILogger<Worker> logger, IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consume() blocks. Yield first so the host can finish starting up.
        await Task.Yield();

        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_options.Topic);
        _logger.LogInformation(
            "Subscribed to {Topic} as group {Group}",
            _options.Topic, _options.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);

                _logger.LogInformation(
                    "Received key={Key} partition={Partition} offset={Offset}",
                    result.Message.Key,
                    result.Partition.Value,
                    result.Offset.Value);

                consumer.Commit(result);   // naive on purpose: one commit per message
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Shutdown requested");
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Consume failed: {Reason}", ex.Error.Reason);
        }
        finally
        {
            consumer.Close();   // leaves the group cleanly
        }
    }
}