using System;
using System.Collections.Generic;
using System.Threading.Tasks; // ✅ Add this for async Task support
using Iwco.LogicGate.Models.Records;

namespace Iwco.LogicGate.Models.Interfaces
{
    public interface IVendorMasterService
    {
        /// <summary>
        /// Determines the winning source between Monarch and LogicGate for each Master Rollup Name.
        /// </summary>
        List<VendorMaster> DetermineWinners(
            Dictionary<string, MasterRollupMonarchRecords> monarchData,
            Dictionary<string, LogicGateRecordWithSupplier> logicGateData);

        /// <summary>
        /// Saves the Vendor Master records to an Excel file.
        /// </summary>
       //public void SaveResultsToExcel(List<VendorMaster> vendorMasterList);

        /// <summary>
        /// Syncs Vendor Master List to the database asynchronously.
        /// </summary>
        Task SyncVendorMasterListAsync(List<VendorMaster> vendorMasterList, string changedBy); // ✅ Renamed and made async

        Task<(bool ChangesDetected, List<VendorMaster> FinalList)> SyncAndCheckChangesAsync(
    List<VendorMaster> vendorMasterList,
    string changedBy);

        // ✅ NEW version with Verticent support
        List<VendorMaster> DetermineWinners(
            Dictionary<string, MasterRollupMonarchRecords> monarchData,
            Dictionary<string, MasterRollupLogicGateRecords> logicGateData,
            Dictionary<string, MasterRollupVerticentRecords> verticentData);
    }
}
