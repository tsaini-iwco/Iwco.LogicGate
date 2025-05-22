using Iwco.LogicGate.Models.Records;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Interfaces
{
    public interface IMasterRollupMapper
    {
        Task<Dictionary<string, MasterRollupMapping>> LoadMappingsAsync();

    }
}
