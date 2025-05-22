using System.Collections.Generic;
using Iwco.LogicGate.Models.Records;

namespace Iwco.LogicGate.Models.Interfaces
{
    public interface IMonarchDataMapper
    {
        Dictionary<string, MasterRollupMonarchRecords> MapMonarchRecordsToRollup(Dictionary<string, MasterRollupMapping> rollupMappings, List<MonarchSupplier> monarchRecords);
    }
}
