using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Iwco.LogicGate.Models.Records;
using Iwco.LogicGate.Connections;

namespace Iwco.LogicGate.Data.DbClients
{
    public class MonarchDbClient
    {
        private readonly string _connectionString;

        public MonarchDbClient(ConnectionStrings connectionStrings)
        {
            // Fetch connection string dynamically
            _connectionString = connectionStrings.GetConnectionString("preprod", "datalake-monarch", out _);
        }

        /// <summary>
        /// Fetches all supplier fields from Monarch database asynchronously.
        /// </summary>
        public async Task<List<MonarchSupplier>> GetAllSuppliersAsync()
        {
            var suppliers = new List<MonarchSupplier>();

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            string sql = @"
                SELECT 
                    sn.[Supplier-Code] AS SupplierCode,
                    sn.[Supplier-Name] AS SupplierName,
                    sn.[Group-ID] AS GroupID,  
                    sg.[Description] AS GroupDescription, 
                    sn.[Active] AS IsActive,
                    sn.[Update-date] AS UpdateDate,
                    sn.[Update-time] AS UpdateTime
                FROM [dbo].[suppname] sn
                JOIN [dbo].[supp-group] sg 
                    ON sn.[Group-ID] = sg.[Group-ID]";

            using var command = new SqlCommand(sql, connection);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var supplier = new MonarchSupplier
                {
                    SupplierCode = reader["SupplierCode"].ToString(),
                    SupplierName = reader["SupplierName"].ToString(),
                    GroupId = reader["GroupID"].ToString(),
                    GroupDescription = reader["GroupDescription"].ToString(),
                    IsActive = reader["IsActive"] != DBNull.Value && (bool)reader["IsActive"],
                    UpdateDate = reader["UpdateDate"] == DBNull.Value ? null : (DateTime?)reader["UpdateDate"],
                    UpdateTime = reader["UpdateTime"] == DBNull.Value ? null : reader["UpdateTime"].ToString()

                };

                suppliers.Add(supplier);
            }

            return suppliers;
        }
    }
}
