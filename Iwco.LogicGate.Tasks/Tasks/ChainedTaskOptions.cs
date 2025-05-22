using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// e.g. in a new file "ChainedTaskOptions.cs" or inside the same tasks folder
namespace Iwco.LogicGate.Tasks.Tasks
{
    public record ChainedTaskOptions(string Environment) : TaskOptions;
}

