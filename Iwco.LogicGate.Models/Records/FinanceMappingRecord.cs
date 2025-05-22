using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class FinanceMappingRecord
    {
        public string SupplierID { get; set; }
        public string SupplierName { get; set; }
        public string MasterRollupNaming { get; set; }
        public string SourceSystem { get; set; }   // “Verticent”, “Monarch”, etc.
        public string SupplierGroupID { get; set; }
        public string SupplierGroupDescription { get; set; }
        public string ActiveYN { get; set; }   // “Y” / “N”
    }
}