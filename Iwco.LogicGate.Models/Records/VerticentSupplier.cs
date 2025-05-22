using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class VerticentSupplier
    {
        public string SupplierId { get; set; }
        public string SupplierName { get; set; }
        public string GroupId { get; set; }
        public string GroupDescription { get; set; }
        public bool IsActive { get; set; }
        public DateTime? UpdateDate { get; set; } = null;
        public string UpdateTime { get; set; } = null;
    }

}
