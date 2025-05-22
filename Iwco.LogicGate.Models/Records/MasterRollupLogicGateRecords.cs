using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class MasterRollupLogicGateRecords
    {
        public string ErpMasterRollupName { get; set; }
        public List<LogicGateSupplier> LogicGateRecords { get; set; } = new();
    }
}

