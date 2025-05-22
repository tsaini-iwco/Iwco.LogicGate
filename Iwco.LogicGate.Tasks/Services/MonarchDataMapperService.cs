using System;
using System.Collections.Generic;
using System.Linq;
using Iwco.LogicGate.Models.Interfaces;
using Iwco.LogicGate.Models.Records;

namespace Iwco.LogicGate.Tasks.Services
{
    public class MonarchDataMapperService : IMonarchDataMapper
    {
        public Dictionary<string, MasterRollupMonarchRecords> MapMonarchRecordsToRollup(
            Dictionary<string, MasterRollupMapping> rollupMappings,
            List<MonarchSupplier> monarchRecords)
        {
            var mappedData = new Dictionary<string, MasterRollupMonarchRecords>();

            foreach (var monarchRecord in monarchRecords)
            {
                // Find if Monarch's SupplierName exists in any rollup's SupplierNames list
                var matchingRollup = rollupMappings
                    .FirstOrDefault(r => r.Value.SupplierNames
                        .Any(s => string.Equals(s, monarchRecord.SupplierName, StringComparison.OrdinalIgnoreCase)));

                if (matchingRollup.Key != null) // Found a match
                {
                    string masterRollupName = matchingRollup.Key;

                    // If this rollup doesn't exist in the final dictionary, create it
                    if (!mappedData.ContainsKey(masterRollupName))
                    {
                        mappedData[masterRollupName] = new MasterRollupMonarchRecords
                        {
                            MasterRollupName = masterRollupName
                        };
                    }

                    // Add Monarch record under this rollup
                    mappedData[masterRollupName].MonarchRecords.Add(monarchRecord);
                }
            }

            return mappedData;
        }
    }
}
