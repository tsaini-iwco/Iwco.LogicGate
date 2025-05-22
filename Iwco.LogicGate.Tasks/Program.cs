using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Iwco.LogicGate.Tasks;          
using Iwco.LogicGate.Data;           
using Iwco.LogicGate.Tasks.Tasks;   
using Iwco.LogicGate.Data.DbClients;
using Iwco.LogicGate.Models.Interfaces;
using Iwco.LogicGate.Tasks.Services;
using Iwco.LogicGate.Connections;
using Iwco.LogicGate.Tasks.Services.Parquet;
using Microsoft.Extensions.Logging;

// 1) Create the default Host builder
var builder = Host.CreateDefaultBuilder(args);

// 2) Decide where logs will be written
var baseFolder = Environment.GetEnvironmentVariable("BASE_FOLDER") ?? @"E:\Data";
var logFolder = System.IO.Path.Combine(baseFolder, "LOGS", "Iwco.LogicGate");
Directory.CreateDirectory(logFolder);

// 3) Configure Serilog with daily rolling log files
var loggerConfig = new LoggerConfiguration();
if (!args.Contains("--no-console"))
{
    loggerConfig = loggerConfig.WriteTo.Console();
}

loggerConfig = loggerConfig.WriteTo.File(
    path: System.IO.Path.Combine(logFolder, "log_.txt"),
    rollingInterval: RollingInterval.Day,
    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}",
    fileSizeLimitBytes: 10 * 1024 * 1024,
    retainedFileCountLimit: 30,
    rollOnFileSizeLimit: true
);

Log.Logger = loggerConfig.CreateLogger();

// 4) JSON serializer options (optional)
var jsonSerializerOptions = new JsonSerializerOptions
{
    AllowTrailingCommas = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    PropertyNameCaseInsensitive = true,
    WriteIndented = true,
    NumberHandling = JsonNumberHandling.AllowReadingFromString,
    PropertyNamingPolicy = null,
    DictionaryKeyPolicy = null
};

// 5) Parse environment channel from command-line args
var channel = GetChannel(args);

try
{
    builder.UseSerilog();

    builder.ConfigureServices((hostContext, services) =>
    {
        var options = ConnectionStringOptions.Default
            .WithProductionWriter(args)
            .WithEnvironment();

        services.AddSingleton(options);
        services.AddSingleton<ConnectionStrings>();

        // Register JSON serializer options
        services.AddSingleton(jsonSerializerOptions);

        // Register the channel (environment)
        services.AddSingleton(channel);

        // Register Database Clients
        services.AddSingleton<LogicGateDbClient>();
        services.AddSingleton<MonarchDbClient>();

        services.AddSingleton<FinanceMappingLoaderService>();
        services.AddSingleton<FinanceMappingDbClient>();

        // Register Data Mapping Services
        services.AddSingleton<IMasterRollupMapper, MasterRollupMapperService>();
        services.AddSingleton<IMonarchDataMapper, MonarchDataMapperService>();
        services.AddSingleton<IVendorMasterService, VendorMasterService>();


        services.AddSingleton<VerticentDataLoader>();
        // Register AzureBlobUploader
        services.AddSingleton<AzureBlobUploader>(provider =>
        {
            var logger = provider.GetRequiredService<ILogger<AzureBlobUploader>>();
            var conn = provider.GetRequiredService<ConnectionStrings>();
            return new AzureBlobUploader(logger, conn, "prod", "vendor-master"); // 👈 manual channel
        });


        // Register LogicGateSyncTask Options
        services.AddSingleton<LogicGateSyncTaskOptions>(new LogicGateSyncTaskOptions(channel));

        // Register VendorMasterSyncTask Options
        services.AddSingleton<VendorMasterSyncTaskOptions>(new VendorMasterSyncTaskOptions(channel));

        // Register LogicGateApiReader with a Bearer token (read from env var if available)
        services.AddSingleton<LogicGateApiReader>(provider =>
        {
            var token = Environment.GetEnvironmentVariable("LOGICGATE_API_TOKEN")
                         ?? "Bearer TTVhcUhUazE6MXBuQXl6UzdVSFdrcGY1OTl3akVrY2JPNGNhQ1gzeEw";
            return new LogicGateApiReader(token);
        });

        // Register LogicGateDatabase using ConnectionStrings (No Hardcoded Connection String)
        services.AddSingleton<LogicGateDatabase>(provider =>
        {
            var connectionStrings = provider.GetRequiredService<ConnectionStrings>();
            return new LogicGateDatabase(connectionStrings);
        });

        // Register VendorMasterDbClient using ConnectionStrings
        services.AddSingleton<VendorMasterDbClient>(provider =>
        {
            var connectionStrings = provider.GetRequiredService<ConnectionStrings>();
            return new VendorMasterDbClient(connectionStrings);
        });

        // Register the LogicGateSyncTask
        services.AddSingleton<TaskBaseTask<LogicGateSyncTaskOptions>, LogicGateSyncTask>();
        services.AddSingleton<LogicGateSyncTask>();

        // Register VendorMasterSyncTask
        services.AddSingleton<TaskBaseTask<VendorMasterSyncTaskOptions>, VendorMasterSyncTask>();
        services.AddSingleton<VendorMasterSyncTask>();

        // ChainedTask setup
        services.AddSingleton<ChainedTaskOptions>(new ChainedTaskOptions(channel));
        services.AddSingleton<TaskBaseTask<ChainedTaskOptions>, ChainedTask>();
        services.AddHostedService<ChainedTask.TaskService>();
    });


    // 6) Build and run the host
    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "The Task failed to start");
}
finally
{
    Log.CloseAndFlush();
}

// Helper function to determine execution environment
static string GetChannel(string[] args)
{
    var channel = "~";
    if (args.Contains("--prod")) channel = "~";
    if (args.Contains("--preprod")) channel = "preprod";
    if (args.Contains("--qa")) channel = "qa";
    if (args.Contains("--dev")) channel = "dev";
    if (args.Contains("--q1")) channel = "q1";
    if (args.Contains("--q2")) channel = "q2";
    if (args.Contains("--d1")) channel = "d1";
    if (args.Contains("--local")) channel = "local";

    return channel;
}
