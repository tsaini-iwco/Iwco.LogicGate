
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Iwco.LogicGate.Connections;
using Iwco.LogicGate.Models.Records;

namespace Iwco.LogicGate.Data.DbClients
{
    public class FinanceMappingDbClient
    {
        private readonly string _connectionString;

        public FinanceMappingDbClient(ConnectionStrings connectionStrings)
        {
            _connectionString = connectionStrings.GetConnectionString("preprod", "datalake-raw", out _);
        }

        public async Task<List<FinanceMappingRecord>> GetAllAsync()
        {
            var list = new List<FinanceMappingRecord>();

            using var con = new SqlConnection(_connectionString);
            await con.OpenAsync();

            const string sql = "SELECT * FROM dbo.x_FinanceMapping";
            using var cmd = new SqlCommand(sql, con);
            using var rdr = await cmd.ExecuteReaderAsync();

            while (await rdr.ReadAsync())
            {
                list.Add(new FinanceMappingRecord
                {
                    SupplierID = rdr["SupplierID"]?.ToString(),
                    SupplierName = rdr["SupplierName"]?.ToString(),
                    MasterRollupNaming = rdr["MasterRollupNaming"]?.ToString(),
                    SourceSystem = rdr["SourceSystem"]?.ToString(),
                    SupplierGroupID = rdr["SupplierGroupID"]?.ToString(),
                    SupplierGroupDescription = rdr["SupplierGroupDescription"]?.ToString(),
                    ActiveYN = rdr["ActiveYN"]?.ToString()
                });
            }

            return list;
        }
    }
}
