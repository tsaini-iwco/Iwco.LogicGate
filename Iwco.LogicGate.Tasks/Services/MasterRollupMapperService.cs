// REPLACE the old file contents entirely
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Iwco.LogicGate.Data.DbClients;
using Iwco.LogicGate.Models.Interfaces;
using Iwco.LogicGate.Models.Records;

namespace Iwco.LogicGate.Tasks.Services
{
    /// <summary>
    /// Builds roll‑up mappings from dbo.x_FinanceMapping instead of Excel.
    /// </summary>
    public class MasterRollupMapperService : IMasterRollupMapper
    {
        private readonly FinanceMappingDbClient _db;

        public MasterRollupMapperService(FinanceMappingDbClient db)
        {
            _db = db;
        }

        public async Task<Dictionary<string, MasterRollupMapping>> LoadMappingsAsync()
        {
            var rows = await _db.GetAllAsync();
            var mappings = new Dictionary<string, MasterRollupMapping>();

            foreach (var r in rows)
            {
                if (string.IsNullOrWhiteSpace(r.MasterRollupNaming)) continue;

                if (!mappings.TryGetValue(r.MasterRollupNaming, out var map))
                {
                    map = new MasterRollupMapping { MasterRollupName = r.MasterRollupNaming };
                    mappings[r.MasterRollupNaming] = map;
                }

                map.SupplierIDs.Add(r.SupplierID);
                map.SupplierNames.Add(r.SupplierName);
            }

            return mappings;
        }
    }
}
