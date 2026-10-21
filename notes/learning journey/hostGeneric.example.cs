var builder = WebApplication.CreateBuilder(args);

// 1. Configure strongly typed settings (IOptions)
builder.Services.Configure<MiningSettings>(
    builder.Configuration.GetSection("MiningSettings")
);

// 2. Register DI services
builder.Services.AddScoped<IBlockService, BlockService>();

// 3. Register Background Service
builder.Services.AddHostedService<MiningWorker>();

var app = builder.Build();

app.MapGet("/", () => "API is running while worker processes in background.");

app.Run();

// Worker implementation
public class MiningWorker : BackgroundService
{
    private readonly IOptionsMonitor<MiningSettings> _options;
    private readonly IServiceScopeFactory _scopeFactory;

    public MiningWorker(
        IOptionsMonitor<MiningSettings> options, 
        IServiceScopeFactory scopeFactory)
    {
        _options = options;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // Access hot-reloaded configuration
            var threads = _options.CurrentValue.WorkerThreads;

            // Resolve scoped dependency safely inside a singleton
            using (var scope = _scopeFactory.CreateScope())
            {
                var blockService = scope.ServiceProvider.GetRequiredService<IBlockService>();
                await blockService.MineNextBlockAsync(threads, stoppingToken);
            }

            await Task.Delay(10000, stoppingToken);
        }
    }
}