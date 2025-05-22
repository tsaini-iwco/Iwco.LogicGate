using System;

namespace Iwco.LogicGate.Models.Records
{
    /// <summary>
    /// Represents a single record from Monarch or LogicGate.
    /// </summary>
    public class SubRecordDto
    {
        public string? Source { get; set; }           // "Monarch" or "LogicGate"
        public string? SourceSystemId { get; set; }   // e.g. Monarch SupplierCode or LogicGate RecordId
        public string? SupplierName { get; set; }
        public string? GroupId { get; set; }
        public string? GroupDescription { get; set; }
        public bool? IsActive { get; set; }
        public DateTime? UpdateDate { get; set; }
        public string? UpdateTime { get; set; }
    }
}
