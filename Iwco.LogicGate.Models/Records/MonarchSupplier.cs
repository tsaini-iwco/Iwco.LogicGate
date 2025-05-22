using System;

namespace Iwco.LogicGate.Models.Records
{
    public class MonarchSupplier
    {
        public string? SupplierCode { get; set; }     // Supplier-Code
        public string? SupplierName { get; set; }     // Supplier-Name
        public string? GroupId { get; set; }          // Group-ID
        public string? GroupDescription { get; set; } // From dbo.supp-group
        public bool? IsActive { get; set; }           // Active field (Status)

        // New Fields
        public DateTime? UpdateDate { get; set; }     // Update-date
        public string? UpdateTime { get; set; }       // Update-time (VARCHAR(8))
    }
}
