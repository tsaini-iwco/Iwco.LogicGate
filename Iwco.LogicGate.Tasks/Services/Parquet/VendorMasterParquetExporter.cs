using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Iwco.LogicGate.Models.Records;
using Microsoft.Extensions.Logging;

namespace Iwco.LogicGate.Tasks.Services.Parquet
{
    public class VendorMasterParquetExporter
    {
        private readonly ILogger _logger;
        private readonly string _outputFolder;



        public VendorMasterParquetExporter(ILogger logger, string outputFolder)
        {
            _logger = logger;
            _outputFolder = outputFolder;
        }

        public async Task<string> SaveToParquetAsync(List<VendorMaster> vendorMasterList)
        {
            Directory.CreateDirectory(_outputFolder);
            var parquetFilePath = Path.Combine(_outputFolder, "VendorMaster.parquet");

            using var connection = new DuckDBConnection("DataSource=:memory:");
            await connection.OpenAsync();

            using (var create = connection.CreateCommand())
            {
                create.CommandText = @"
                CREATE TABLE VendorMaster (
                    SupplierName VARCHAR,
                    VendorStatus VARCHAR,
                    StatusChangedDate TIMESTAMP,
                    IsDeleted BOOLEAN,
                    IsActive BOOLEAN,
                    SupplierDetails JSON,
                    SourceSystem VARCHAR,
                    ChangedDate TIMESTAMP,
                    ChangedBy VARCHAR,
                );";
                await create.ExecuteNonQueryAsync();
            }

            foreach (var batch in vendorMasterList.Chunk(500))
            {
                foreach (var record in batch)
                {
                    using var insert = connection.CreateCommand();
                    insert.CommandText = $@"
INSERT INTO VendorMaster 
VALUES (
    '{EscapeSql(record.SupplierName)}',
    '{(record.WinnerIsActive == true ? "Active" : "Inactive")}',
    {FormatTimestamp(record.WinnerUpdateDate)},
    FALSE,
    {FormatBool(record.WinnerIsActive)},
    '{EscapeSql(record.SupplierDetails)}',
    '{EscapeSql(record.Source)}',
    {FormatTimestamp(record.UpdatedDateTime)},
    '{EscapeSql(record.WinnerSource)}'
);";
                    await insert.ExecuteNonQueryAsync();
                }
            }

            using (var export = connection.CreateCommand())
            {
                export.CommandText = $"COPY VendorMaster TO '{parquetFilePath}' (FORMAT 'parquet');";
                await export.ExecuteNonQueryAsync();
            }

            _logger.LogInformation("Parquet file saved at: {Path}", parquetFilePath);
            return parquetFilePath;
        }

        private string EscapeSql(string? input)
        {
            return input?.Replace("'", "''") ?? "";
        }

        private string FormatTimestamp(DateTime? dt)
        {
            return dt.HasValue ? $"'{dt:yyyy-MM-dd HH:mm:ss}'" : "NULL";
        }

        private string FormatBool(bool? val)
        {
            return val.HasValue ? (val.Value ? "TRUE" : "FALSE") : "NULL";
        }
    }
}
