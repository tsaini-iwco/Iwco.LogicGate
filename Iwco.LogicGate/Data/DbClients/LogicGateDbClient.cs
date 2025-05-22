using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Iwco.LogicGate.Models.Records;
using Iwco.LogicGate.Connections; 

namespace Iwco.LogicGate.Data.DbClients
{
    public class LogicGateDbClient
    {
        private readonly string _connectionString;

        public LogicGateDbClient(ConnectionStrings connectionStrings)
        {
            // Fetch connection string dynamically
            _connectionString = connectionStrings.GetConnectionString("preprod", "datalake-raw", out _);

        }

        /// <summary>
        /// Retrieves all LogicGate records asynchronously.
        /// </summary>
        public async Task<List<LogicGateRecord>> GetAllRecordsAsync()
        {
            var allRecords = new List<LogicGateRecord>();

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var command = new SqlCommand("SELECT id, dates, fields FROM dbo.lg_records", connection);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var record = new LogicGateRecord
                {
                    Id = reader["id"].ToString(),
                    Dates = JsonConvert.DeserializeObject<LogicGateDates>(reader["dates"].ToString() ?? ""),
                    Fields = JsonConvert.DeserializeObject<List<LogicGateField>>(reader["fields"].ToString() ?? "")
                };

                if (record.Dates != null && record.Fields != null)
                {
                    allRecords.Add(record);
                }
            }

            return allRecords;
        }

        /// <summary>
        /// Retrieves all supplier records asynchronously.
        /// </summary>
        public async Task<List<LogicGateRecordWithSupplier>> GetSupplierRecordsAsync()
        {
            var allRecords = await GetAllRecordsAsync();
            var supplierRecords = allRecords.ConvertAll(LogicGateSupplierFieldsMapper.MapToSupplierFields);
            return supplierRecords;
        }
    }
}
