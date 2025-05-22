using Microsoft.Extensions.Logging;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Iwco.LogicGate.Data;
using Iwco.LogicGate.Tasks.Tasks;
using Microsoft.Extensions.Options;
using ServiceStack.Logging;
using Iwco.LogicGate.Data.DbClients;

namespace Iwco.LogicGate.Tasks
{
    public class LogicGateSyncTask : TaskBaseTask<LogicGateSyncTaskOptions>
    {
        private readonly LogicGateApiReader _apiReader;
        private readonly LogicGateDatabase _database;
        private readonly LogicGateDbClient _dbClient;

        public LogicGateSyncTask(
            ILogger<LogicGateSyncTask> logger,
            LogicGateSyncTaskOptions options,
            LogicGateApiReader apiReader,
            LogicGateDatabase database,
            LogicGateDbClient dbClient   // MUST be the last parameter!
        ) : base(logger, options)
        {
            _apiReader = apiReader;
            _database = database;
            _dbClient = dbClient;
        }

        public override async Task<(bool IsSuccess, bool IsComplete)> OnExecuteStep(CancellationToken stoppingToken)
        {
            TaskLogger.LogInformation("Running LogicGateSyncTask in environment: {Env}", Options.Environment);

            // 1) Fetch ALL paginated data from LogicGate
            JsonArray fullContent = await _apiReader.GetAllRecordsAsync("m7nii2gF", "vrwsSLMG");

            // 2) Log how many records we got
            TaskLogger.LogInformation("LogicGate API returned {Count} records", fullContent.Count);

            // 3) Insert into [dbo].[lg_records]
            _database.CopyRecordsToSql(fullContent);

            // 4) Fetch and Display Supplier Records from DB
           // TaskLogger.LogInformation("Fetching Supplier Records from Database...");
           // await FetchAndDisplaySupplierRecordsAsync();

            // 5) Return that we’re done
            return (true, true);
        }


        /// <summary>
        /// Fetches and displays supplier records using LogicGateDbClient
        /// </summary>
        //public async Task FetchAndDisplaySupplierRecordsAsync()
        //{
        //    var supplierRecords = await _dbClient.GetSupplierRecordsAsync();

        //    TaskLogger.LogInformation($"Total Supplier Records Fetched: {supplierRecords.Count}");
        //    TaskLogger.LogInformation("---- SUPPLIER RECORDS START ----");

        //    foreach (var record in supplierRecords)
        //    {
        //        TaskLogger.LogInformation($"Record ID: {record.RecordId}");
        //        TaskLogger.LogInformation($"Updated Date: {record.UpdatedDate:yyyy-MM-dd HH:mm:ss}");

        //        var supplier = record.SupplierFields;
        //        TaskLogger.LogInformation($"ERP Master Rollup Name: {supplier.ErpMasterRollupName}");
        //        TaskLogger.LogInformation($"Supplier Name: {supplier.SupplierName}");
        //        TaskLogger.LogInformation($"ERP Group ID: {supplier.ErpGroupId}");
        //        TaskLogger.LogInformation($"ERP Group Description: {supplier.ErpGroupDescription}");
        //        TaskLogger.LogInformation($"Preferred Payment Type: {supplier.PreferredPaymentType}");
        //        TaskLogger.LogInformation($"Financial Tier: {supplier.FinancialTier}");
        //        TaskLogger.LogInformation($"Renegotiated Payment Terms: {supplier.RenegotiatedPaymentTerms}");
        //        TaskLogger.LogInformation($"ERP Setup: {supplier.ErpSetup}");
        //        TaskLogger.LogInformation($"Monarch Supplier Number 1: {supplier.MonarchSupplierNumber1}");
        //        TaskLogger.LogInformation($"Monarch Supplier Number 2: {supplier.MonarchSupplierNumber2}");
        //        TaskLogger.LogInformation($"Monarch Supplier Number 3: {supplier.MonarchSupplierNumber3}");
        //        TaskLogger.LogInformation($"Monarch Supplier Number 4: {supplier.MonarchSupplierNumber4}");
        //        TaskLogger.LogInformation("----");
        //    }

        //    TaskLogger.LogInformation("---- SUPPLIER RECORDS END ----");
        //}

    }
}
