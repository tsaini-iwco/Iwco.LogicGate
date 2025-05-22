using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iwco.LogicGate.Connections;
using Microsoft.Extensions.Logging;

namespace Iwco.LogicGate.Tasks.Services;

public class AzureBlobUploader
{
    private readonly ILogger _logger;
    private readonly ConnectionStrings _connectionStrings;
    private readonly string _channel;
    private readonly string _container;

    public AzureBlobUploader(ILogger<AzureBlobUploader> logger, ConnectionStrings connectionStrings, string channel, string container)
    {
        _logger = logger;
        _connectionStrings = connectionStrings;
        _channel = channel;
        _container = container;
    }

    public async Task UploadFileAsync(string filePath)
    {
        //if (_channel != "~")
        //{
        //    _logger.LogInformation($" Skipping Azure upload — current channel is '{_channel}', not production.");
        //    return;
        //}

        var blobConnectionString = _connectionStrings.GetConnectionStringInternal(_channel, "azure-data-blob", out _);

        var blobClientOptions = new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClientHandler())
        };

        var containerClient = new BlobContainerClient(blobConnectionString, _container, blobClientOptions);
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient(Path.GetFileName(filePath));

        var uploadOptions = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = "application/octet-stream"
            }
        };

        var retries = 3;
        while (retries > 0)
        {
            if (File.Exists(filePath))
            {
                try
                {
                    var response = await blobClient.UploadAsync(filePath, uploadOptions);
                    if (response.GetRawResponse().IsError)
                    {
                        _logger.LogError("❌ Upload failed for {FileName}: {Reason}", Path.GetFileName(filePath), response.GetRawResponse().ReasonPhrase);
                        retries--;
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        _logger.LogInformation("✅ Successfully uploaded {FileName} to Azure Blob", Path.GetFileName(filePath));
                        return;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❗ Exception during upload attempt for {FileName}", Path.GetFileName(filePath));
                    retries--;
                    Thread.Sleep(1000);
                }
            }
        }

        _logger.LogError("🧨 All upload attempts failed for {FileName}", Path.GetFileName(filePath));
    }
}
