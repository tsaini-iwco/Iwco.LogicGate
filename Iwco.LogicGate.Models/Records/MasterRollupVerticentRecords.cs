using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public class MasterRollupVerticentRecords
    {
        public string MasterRollupName { get; set; }
        public List<VerticentSupplier> VerticentRecords { get; set; } = new List<VerticentSupplier>();
    }
}
