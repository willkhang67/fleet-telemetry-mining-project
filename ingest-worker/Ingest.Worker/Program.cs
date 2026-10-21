using Ingest.Worker;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddHostedService<Worker>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok"  }));

app.Run();