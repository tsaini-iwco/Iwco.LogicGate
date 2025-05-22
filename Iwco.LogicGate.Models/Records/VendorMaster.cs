using System;

namespace Iwco.LogicGate.Models.Records
{
    public class VendorMaster
    {
        public int Id { get; set; }                      // DB auto-increment
        public string SupplierName { get; set; }         // Master Rollup Name
        public string Source { get; set; }               // Winner source (e.g. "LogicGate" or "Monarch")
        public DateTime UpdatedDateTime { get; set; }      // Winner update date/time
        public string SupplierDetails { get; set; }      // JSON array of all sub-records

        // Additional properties for winner info (used for DB columns)
        public bool? WinnerIsActive { get; set; }
        public DateTime? WinnerUpdateDate { get; set; }
        public string WinnerSource { get; set; }
    }
}
