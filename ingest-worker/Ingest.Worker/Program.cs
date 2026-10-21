using Ingest.Worker;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok"  }));

app.Run();