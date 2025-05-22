using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class MasterRollupMapping
    {
        public string MasterRollupName { get; set; } = string.Empty; // e.g., "CINTAS CORP."
        public List<string> SupplierIDs { get; set; } = new();
        public List<string> SupplierNames { get; set; } = new();
    }
}
