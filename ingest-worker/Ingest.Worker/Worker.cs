using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Ingest.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly KafkaOptions _options;
    private readonly IHostApplicationLifetime _lifetime;

    public Worker(
        ILogger<Worker> logger,
        IOptions<KafkaOptions> options,
        IHostApplicationLifetime lifetime)
    {
        _logger = logger;
        _options = options.Value;
        _lifetime = lifetime;
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
            EnableAutoCommit = false   // we commit ourselves, after handling the message
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_options.Topic);
        _logger.LogInformation(
            "Subscribed to {Topic} as group {Group}, waiting for one message...",
            _options.Topic, _options.GroupId);

        try
        {
            var result = consumer.Consume(stoppingToken);

            _logger.LogInformation(
                "Received key={Key} partition={Partition} offset={Offset} value={Value}",
                result.Message.Key,
                result.Partition.Value,
                result.Offset.Value,
                result.Message.Value);

            consumer.Commit(result);   // commit only after the message is handled
            _logger.LogInformation("Committed offset {Offset}", result.Offset.Value + 1);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Shutdown requested before a message arrived");
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Consume failed: {Reason}", ex.Error.Reason);
        }
        finally
        {
            consumer.Close();   // leaves the group cleanly
        }

        _lifetime.StopApplication();
    }
}