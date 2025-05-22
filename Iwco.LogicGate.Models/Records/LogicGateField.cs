using System.Collections.Generic;

namespace Iwco.LogicGate.Models.Records
{
    public class LogicGateField
    {
        public string Id { get; set; }
        public string? Name { get; set; }
        public string? Label { get; set; }
        public bool Global { get; set; }
        public string? Type { get; set; }
        public string? ValueType { get; set; }
        public bool Required { get; set; }
        public List<LogicGateFieldValue> Values { get; set; }
        public string Object { get; set; }
    }
}
