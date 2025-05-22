using Iwco.LogicGate.Models.Records;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Tasks.Services
{
    public class VerticentDataMapper
    {
        public Dictionary<string, MasterRollupVerticentRecords> MapToRollups(
            Dictionary<string, MasterRollupMapping> rollupMappings,
            List<VerticentSupplier> verticentSuppliers)
        {
            var result = new Dictionary<string, MasterRollupVerticentRecords>();

            foreach (var supplier in verticentSuppliers)
            {
                var matchingRollup = rollupMappings
                    .FirstOrDefault(r => r.Value.SupplierNames
                        .Any(name => string.Equals(name, supplier.SupplierName, StringComparison.OrdinalIgnoreCase)));

                if (matchingRollup.Key != null)
                {
                    string masterRollup = matchingRollup.Key;

                    if (!result.ContainsKey(masterRollup))
                    {
                        result[masterRollup] = new MasterRollupVerticentRecords
                        {
                            MasterRollupName = masterRollup
                        };
                    }

                    result[masterRollup].VerticentRecords.Add(supplier);
                }
            }

            return result;
        }
    }

}
