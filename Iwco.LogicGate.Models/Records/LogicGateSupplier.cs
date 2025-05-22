using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class LogicGateSupplier
    {
        public string SupplierId { get; set; }           // <-- This will be RecordId
        public string SupplierName { get; set; }
        public string GroupId { get; set; }
        public string GroupDescription { get; set; }
        public bool IsActive { get; set; } = true;
        public DateTime? UpdateDate { get; set; }
        public string UpdateTime { get; set; }
    }

}
