using System;

namespace Iwco.LogicGate.Models.Records
{
    public class LogicGateRecordWithSupplier
    {
        public string RecordId { get; set; }  // Links to lg_records.id
        public DateTime? UpdatedDate { get; set; }  // Extracted from `dates` field
        public LogicGateSupplierFields SupplierFields { get; set; }  // Extracted supplier fields
    }
}
