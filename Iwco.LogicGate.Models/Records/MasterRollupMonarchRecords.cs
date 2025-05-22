using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class MasterRollupMonarchRecords
    {
        public string MasterRollupName { get; set; } = string.Empty; // e.g., "CINTAS CORP."
        public List<MonarchSupplier> MonarchRecords { get; set; } = new();
    }
}
