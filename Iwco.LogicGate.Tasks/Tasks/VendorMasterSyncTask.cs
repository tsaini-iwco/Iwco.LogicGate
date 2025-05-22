using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Iwco.LogicGate.Tasks.Tasks;
using Iwco.LogicGate.Data.DbClients;
using Iwco.LogicGate.Models.Interfaces;
using Iwco.LogicGate.Models.Records;
using Iwco.LogicGate.Tasks.Services;
using Iwco.LogicGate.Tasks.Services.Parquet;

namespace Iwco.LogicGate.Tasks
{
    public class VendorMasterSyncTask : TaskBaseTask<VendorMasterSyncTaskOptions>
    {
        private readonly IMasterRollupMapper _rollupMapper;
        private readonly IMonarchDataMapper _monarchMapper;
        private readonly IVendorMasterService _vendorMasterService;
        private readonly MonarchDbClient _monarchDbClient;
        private readonly LogicGateDbClient _logicGateDbClient;
        private readonly AzureBlobUploader _blobUploader;
        private readonly FinanceMappingLoaderService _financeMappingLoader;
        private readonly VerticentDataLoader _verticentLoader;

        
        public VendorMasterSyncTask(
            ILogger<VendorMasterSyncTask> logger,
            VendorMasterSyncTaskOptions options,
            IMasterRollupMapper rollupMapper,
            IMonarchDataMapper monarchMapper,
            IVendorMasterService vendorMasterService,
            MonarchDbClient monarchDbClient,
            LogicGateDbClient logicGateDbClient,
            AzureBlobUploader blobUploader,
            FinanceMappingLoaderService financeMappingLoader,
            VerticentDataLoader verticentLoader
        ) : base(logger, options)
        {
            _rollupMapper = rollupMapper;
            _monarchMapper = monarchMapper;
            _vendorMasterService = vendorMasterService;
            _monarchDbClient = monarchDbClient;
            _logicGateDbClient = logicGateDbClient;
            _blobUploader = blobUploader;
            _financeMappingLoader = financeMappingLoader;
            _verticentLoader = verticentLoader;
        }

        
        public override async Task<(bool IsSuccess, bool IsComplete)> OnExecuteStep(
            CancellationToken stoppingToken)
        {
            TaskLogger.LogInformation("Running VendorMasterSyncTask …");

            // ── locate workbook (no move yet)
            var excelFile = FinanceMappingFileLocator.TryGetFile(out bool fromReadDir);
            var baseFolder = Environment.GetEnvironmentVariable("BASE_FOLDER") ?? @"E:\Data";
            var filesFolder = Path.Combine(baseFolder, "FILES", "Iwco.LogicGate.Tasks");
            var outputBase = filesFolder;               

            try
            {
                //Refresh staging table
                await _financeMappingLoader.LoadAsync(excelFile);
                TaskLogger.LogInformation("x_FinanceMapping refreshed from Excel.");

                //Roll‑up mappings (from SQL)
                TaskLogger.LogInformation("Building Master Rollup mappings …");
                var rollupMappings = await _rollupMapper.LoadMappingsAsync();
                TaskLogger.LogInformation("Loaded {Count} mappings.", rollupMappings.Count);

                //Monarch
                TaskLogger.LogInformation("Fetching Monarch records …");
                var monarchRecords = await _monarchDbClient.GetAllSuppliersAsync();
                TaskLogger.LogInformation("Fetched {Count} Monarch records.", monarchRecords.Count);

                //Map Monarch to Roll‑up
                TaskLogger.LogInformation("Mapping Monarch to Roll‑ups …");
                var mappedMonarch = _monarchMapper.MapMonarchRecordsToRollup(rollupMappings, monarchRecords);
                TaskLogger.LogInformation("Mapped {Count} roll‑up names from Monarch.", mappedMonarch.Count);

                // LogicGate
                TaskLogger.LogInformation("Fetching LogicGate records …");
                var logicGateRecords = await _logicGateDbClient.GetSupplierRecordsAsync();
                TaskLogger.LogInformation("Fetched {Count} LogicGate records.", logicGateRecords.Count);

                var lgMapper = new LogicGateDataMapper();
                var mappedLogicGate = lgMapper.MapToRollups(logicGateRecords);

                //  Verticent (from DataLoad table SQL)
                TaskLogger.LogInformation("Loading Verticent records …");
                var verticentRecords = await _verticentLoader.LoadVerticentSuppliersAsync();
                TaskLogger.LogInformation("Loaded {Count} Verticent records.", verticentRecords.Count);

                var verticentMapper = new VerticentDataMapper();
                var mappedVerticent = verticentMapper.MapToRollups(rollupMappings, verticentRecords);
                TaskLogger.LogInformation("Mapped {Count} roll‑up names from Verticent.", mappedVerticent.Count);

                //  Determine winners
                TaskLogger.LogInformation("Determining winners …");
                var vendorMasterList = _vendorMasterService.DetermineWinners(
                                            mappedMonarch,
                                            mappedLogicGate,
                                            mappedVerticent);
                TaskLogger.LogInformation("Determined {Count} records (with winners).", vendorMasterList.Count);

                // Sync to DB
                TaskLogger.LogInformation("Syncing VendorMasterList to DB …");
                var (changesDetected, finalList) =
                    await _vendorMasterService.SyncAndCheckChangesAsync(
                        vendorMasterList, "VendorMasterSyncTask");

                // Parquet + Azure
                TaskLogger.LogInformation("Exporting VendorMasterList to Parquet …");
                var parquetExporter = new VendorMasterParquetExporter(TaskLogger, outputBase);
                var parquetPath = await parquetExporter.SaveToParquetAsync(finalList);

                if (changesDetected)
                {
                    TaskLogger.LogInformation("Changes detected—uploading Parquet to Azure …");
                    await _blobUploader.UploadFileAsync(parquetPath);
                }
                else
                {
                    TaskLogger.LogInformation("No changes detected—skipping Azure upload.");
                }

                TaskLogger.LogInformation("VendorMasterSyncTask complete.");

                //  file only after SUCCESS
                if (fromReadDir)
                {
                    FinanceMappingFileLocator.ArchiveReadFile(excelFile);
                    TaskLogger.LogInformation("Workbook archived to Processed folder.");
                }
                return (true, true);
            }
            catch (Exception ex)
            {
                TaskLogger.LogError(ex, "VendorMasterSyncTask failed.");
                throw;  
            }
        }
    }
}
