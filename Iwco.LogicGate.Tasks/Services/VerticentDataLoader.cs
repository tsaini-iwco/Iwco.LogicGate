// REPLACE the old file contents entirely
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Iwco.LogicGate.Data.DbClients;
using Iwco.LogicGate.Models.Records;

namespace Iwco.LogicGate.Tasks.Services
{
    public class VerticentDataLoader
    {
        private readonly FinanceMappingDbClient _db;
        public VerticentDataLoader(FinanceMappingDbClient db) => _db = db;

        public async Task<List<VerticentSupplier>> LoadVerticentSuppliersAsync()
        {
            var rows = await _db.GetAllAsync();

            var list = rows
                .Where(r => string.Equals(r.SourceSystem, "Verticent", StringComparison.OrdinalIgnoreCase))
                .Select(r => new VerticentSupplier
                {
                    SupplierId = r.SupplierID,
                    SupplierName = r.SupplierName,
                    GroupId = r.SupplierGroupID,
                    GroupDescription = r.SupplierGroupDescription,
                    IsActive = string.Equals(r.ActiveYN, "Y", StringComparison.OrdinalIgnoreCase),
                    UpdateDate = null,
                    UpdateTime = null
                })
                .ToList();

            return list;
        }
    }
}
