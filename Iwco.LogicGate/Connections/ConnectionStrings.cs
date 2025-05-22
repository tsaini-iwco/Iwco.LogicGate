using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Iwco.LogicGate.Connections;

public record ConnectionStringOptions(bool EnableProduction, bool UseTestDefault, bool UseDevDefault, string BaseFolder, string? AppId = null, string? AppVersion = null)
{
    public static ConnectionStringOptions Default => new(false, true, false, "E:\\Data");
    public ConnectionStringOptions WithProductionWriter(string[] args)
    {
        var enableProduction = false;
        for (int i = 1; i < args.Length; i++)
        {
            if (args[i - 1] == "--writers")
            {
                enableProduction = args[i].Equals(Environment.MachineName, StringComparison.OrdinalIgnoreCase);
            }
        }

        return this with { EnableProduction = enableProduction };
    }

    public ConnectionStringOptions WithEnvironment()
    {
        var baseFolder = Environment.GetEnvironmentVariable("BASE_FOLDER") ?? BaseFolder;
        return this with { BaseFolder = baseFolder };
    }

}

public class ConnectionStrings
{
    IConfiguration? _configuration;
    IHostEnvironment? _environment;
    ConcurrentDictionary<string, string> _connectionStrings = new(StringComparer.OrdinalIgnoreCase);
    ConcurrentDictionary<string, bool> _featureFlags = new(StringComparer.OrdinalIgnoreCase);

    readonly ConnectionStringOptions _options;
    private bool _isProduction;
    private string _applicationName;

    public bool EnableProduction => _options.EnableProduction;
    public bool UseTestDefault => _options.UseTestDefault;
    public bool UseDevDefault => _options.UseDevDefault;
    public string BaseFolder => _options.BaseFolder;

    public bool AllowWrite(bool isProduction)
    {
        if (isProduction && !EnableProduction) return false;
        return true;
    }

    public ConnectionStrings(ConnectionStringOptions options, bool isProduction, string applicationName)
    {
        _environment = null;
        _configuration = null;
        _options = options;
        _isProduction = isProduction;
        _applicationName = applicationName;
    }


    public ConnectionStrings(IHostEnvironment environment, IConfiguration configuration, ConnectionStringOptions options)
    {
        _configuration = configuration;
        _options = options;
        _environment = environment;

        _isProduction = !_environment.IsDevelopment();
        _applicationName = _environment.ApplicationName;
    }

    public string GetChannelOrDefault(string? channel)
    {
        if (string.IsNullOrEmpty(channel) || channel == "~")
        {
            if (_isProduction) { channel = "prod"; }
            else if (UseTestDefault) { channel = "qa"; }
            else if (UseDevDefault) { channel = "dev"; }
            else channel = "prod";
        }

        return channel;
    }

    public bool AllowFeature(string? channel, string name)
    {
        channel = GetChannelOrDefault(channel);
        var key = $"{name}:{channel}";
        if (_featureFlags.TryGetValue(key, out var value)) return value;

        var envkey = $"{name}_{channel}_FEATUREFLAG".ToUpperInvariant();
        var flag = Environment.GetEnvironmentVariable(envkey);
        if (flag is not null)
        {
            _featureFlags[key] = true;
            return true;
        }

        envkey = $"{name}_FEATUREFLAG".ToUpperInvariant();
        flag = Environment.GetEnvironmentVariable(envkey);
        if (flag is not null)
        {
            _featureFlags[key] = true;
            return true;
        }

        _featureFlags[key] = false;
        return false;
    }

    public bool AllowFeature(string? channel, string name, string group)
    {
        channel = GetChannelOrDefault(channel);
        var key = $"{name}:{channel}:{group}";
        if (_featureFlags.TryGetValue(key, out var value)) return value;

        var envkey = $"{name}_{channel}_{group}_FEATUREFLAG".ToUpperInvariant();
        var flag = Environment.GetEnvironmentVariable(envkey);
        if (flag is not null)
        {
            _featureFlags[key] = true;
            return true;
        }

        envkey = $"{name}_{group}_FEATUREFLAG".ToUpperInvariant();
        flag = Environment.GetEnvironmentVariable(envkey);
        if (flag is not null)
        {
            _featureFlags[key] = true;
            return true;
        }

        _featureFlags[key] = false;
        return false;
    }

    public string GetConnectionString(string? channel, string name, out bool isProduction, bool useIntegratedSecurity = false) => GetConnectionString(channel, name, _applicationName, out isProduction, useIntegratedSecurity);
    public string GetConnectionString(string? channel, string name, string applicationName, out bool isProduction, bool useIntegratedSecurity = false)
    {
        var result = GetConnectionStringInternal(channel, name, out isProduction);
        if (useIntegratedSecurity)
        {
            var idx = result.IndexOf("User ID=");
            if (idx != -1)
            {
                result = $"{result[..idx].TrimEnd(';')};Integrated Security=true;";
            }
        }

        if (!result.Contains("Application Name=") && !string.IsNullOrEmpty(applicationName))
            result = $"{result.TrimEnd(';')};Application Name={applicationName};";

        return result;
    }


    public string GetConnectionStringInternal(string? channel, string name, out bool isProduction)
    {
        channel = GetChannelOrDefault(channel);
        isProduction = channel == "prod";
        var key = $"{name}:{channel}";

        if (_connectionStrings.TryGetValue(key, out var cs)) return cs;

        cs = _configuration?.GetConnectionString(name);
        if (cs is not null)
        {
            _connectionStrings[key] = cs;
            return cs;
        }

        var envkey = $"{name}_{channel}_CONNECTIONSTRING".ToUpperInvariant();
        cs = Environment.GetEnvironmentVariable(envkey);
        if (cs is not null)
        {
            _connectionStrings[key] = cs;
            return cs;
        }

        envkey = $"{name}_CONNECTIONSTRING".ToUpperInvariant();
        cs = Environment.GetEnvironmentVariable(envkey);
        if (cs is not null)
        {
            _connectionStrings[key] = cs;
            return cs;
        }

        var configpath = Path.Combine(BaseFolder, "CONFIG", _applicationName, "connections.json");
        if (File.Exists(configpath) && TryReadFile(configpath, channel, name, key, out cs))
            return cs!;

        configpath = Path.Combine(BaseFolder, "CONFIG", "connections.json");
        if (File.Exists(configpath) && TryReadFile(configpath, channel, name, key, out cs))
            return cs!;

        configpath = Path.Combine("C:\\IWCOApps", "CONFIG", "connections.json");
        if (File.Exists(configpath) && TryReadFile(configpath, channel, name, key, out cs))
            return cs!;

        throw new KeyNotFoundException($"Could not find connection string for {name} [{channel}]");
    }

    private bool TryReadFile(string filename, string channel, string name, string key, out string? cs)
    {
        cs = null;
        var json = File.ReadAllText(filename);
        var obj = JsonSerializer.Deserialize<Dictionary<string, JsonNode>>(json);
        if (obj is Dictionary<string, JsonNode> map)
        {
            if (map.TryGetValue(channel, out var dict))
            {
                if (dict is JsonObject jobj)
                {
                    cs = jobj[name]?.ToString();
                    if (cs is not null)
                    {
                        _connectionStrings[key] = cs;
                        return true;
                    }
                }
            }

            if (map.TryGetValue(name, out var node))
            {
                cs = node?.ToString();
                if (cs is not null)
                {
                    _connectionStrings[key] = cs;
                    return true;
                }
            }
        }

        return false;
    }
}

public record ConnectionChannel(string Channel, bool IsProduction);
