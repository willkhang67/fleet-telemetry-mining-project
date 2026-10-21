namespace Ingest.Worker;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "127.0.0.1:9094";
    public string Topic { get; set; } = "telemetry";
    public string GroupId { get; set; } = "ingest-worker";
    // public string ClientId { get; set; } = "ingest-worker";
    // public string SecurityProtocol { get; set; } = "PLAINTEXT";
    // public string SaslMechanisms { get; set; } = "PLAIN";
    // public string SaslJaasConfig { get; set; } = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";";
    // public string SslEndpointIdentificationAlgorithm { get; set; } = "https";
    // public string SslTruststoreLocation { get; set; } = "/etc/kafka/secrets/kafka.client.truststore.jks";
    // public string SslTruststorePassword { get; set; } = "admin-secret";
    // public string SslKeystoreLocation { get; set; } = "/etc/kafka/secrets/kafka.client.keystore.jks";
    // public string SslKeystorePassword { get; set; } = "admin-secret";
}