// Assume all namespaces are already imported and available.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Iwco.LogicGate.Models.Records;
using Iwco.LogicGate.Models.Interfaces;
using Iwco.LogicGate.Data.DbClients;

namespace Iwco.LogicGate.Tasks.Services
{
    public class VendorMasterService : IVendorMasterService
    {
        private readonly VendorMasterDbClient _dbClient;

        public VendorMasterService(VendorMasterDbClient dbClient)
        {
            _dbClient = dbClient;
        }

        // Interface-compatible method (Legacy)
        public List<VendorMaster> DetermineWinners(
            Dictionary<string, MasterRollupMonarchRecords> monarchData,
            Dictionary<string, LogicGateRecordWithSupplier> logicGateData)
        {
            // Wrap legacy logicGateData into the new structure with one record each
            var transformedLogicGate = logicGateData.ToDictionary(
                kvp => kvp.Key,
                kvp => new MasterRollupLogicGateRecords
                {
                    ErpMasterRollupName = kvp.Key,
                    LogicGateRecords = new List<LogicGateSupplier> {
                        new LogicGateSupplier
                        {
                            SupplierId = kvp.Value.RecordId,
                            SupplierName = kvp.Value.SupplierFields.SupplierName,
                            GroupId = kvp.Value.SupplierFields.ErpGroupId,
                            GroupDescription = kvp.Value.SupplierFields.ErpGroupDescription,
                            IsActive = kvp.Value.SupplierFields.IsActive,
                            UpdateDate = kvp.Value.UpdatedDate?.Date,
                            UpdateTime = kvp.Value.UpdatedDate?.ToString("HH:mm:ss")
                        }
                    }
                });

            return DetermineWinners(monarchData, transformedLogicGate, new());
        }

        // New extended version with Verticent support
        public List<VendorMaster> DetermineWinners(
            Dictionary<string, MasterRollupMonarchRecords> monarchData,
            Dictionary<string, MasterRollupLogicGateRecords> logicGateData,
            Dictionary<string, MasterRollupVerticentRecords> verticentData)
        {
            var vendorMasterList = new List<VendorMaster>();

            var allRollupNames = monarchData.Keys
                .Union(logicGateData.Keys)
                .Union(verticentData.Keys)
                .Distinct();

            foreach (var rollupName in allRollupNames)
            {
                monarchData.TryGetValue(rollupName, out var monarchRecords);
                logicGateData.TryGetValue(rollupName, out var logicGateRecords);
                verticentData.TryGetValue(rollupName, out var verticentRecords);

                var subRecords = new List<SubRecordDto>();

                // Monarch Records
                if (monarchRecords?.MonarchRecords != null)
                {
                    foreach (var m in monarchRecords.MonarchRecords)
                    {
                        subRecords.Add(ToSubRecord("Monarch", m.SupplierCode, m.SupplierName, m.GroupId, m.GroupDescription, m.IsActive ?? false, m.UpdateDate, m.UpdateTime));
                    }
                }

                // LogicGate Records
                if (logicGateRecords?.LogicGateRecords != null)
                {
                    foreach (var l in logicGateRecords.LogicGateRecords)
                    {
                        subRecords.Add(ToSubRecord("LogicGate", l.SupplierId, l.SupplierName, l.GroupId, l.GroupDescription, l.IsActive, l.UpdateDate, l.UpdateTime));
                    }
                }

                // Verticent Records (only included in SupplierDetails)
                if (verticentRecords?.VerticentRecords != null)
                {
                    foreach (var v in verticentRecords.VerticentRecords)
                    {
                        subRecords.Add(ToSubRecord("Verticent", v.SupplierId, v.SupplierName, v.GroupId, v.GroupDescription, v.IsActive, v.UpdateDate, v.UpdateTime));
                    }
                }

                // Determine winner from Monarch and LogicGate only
                var monarchLatest = subRecords
                    .Where(r => r.Source == "Monarch" && r.UpdateDate.HasValue)
                    .OrderByDescending(r => CombineDateTime(r.UpdateDate, r.UpdateTime))
                    .FirstOrDefault();

                var logicGateLatest = subRecords
                    .Where(r => r.Source == "LogicGate" && r.UpdateDate.HasValue)
                    .OrderByDescending(r => CombineDateTime(r.UpdateDate, r.UpdateTime))
                    .FirstOrDefault();

                var monarchTime = CombineDateTime(monarchLatest?.UpdateDate, monarchLatest?.UpdateTime);
                var logicGateTime = CombineDateTime(logicGateLatest?.UpdateDate, logicGateLatest?.UpdateTime);

                SubRecordDto? winnerRecord = null;

                if (monarchTime.HasValue && logicGateTime.HasValue)
                {
                    winnerRecord = monarchTime > logicGateTime ? monarchLatest : logicGateLatest;
                }
                else if (monarchTime.HasValue)
                {
                    winnerRecord = monarchLatest;
                }
                else if (logicGateTime.HasValue)
                {
                    winnerRecord = logicGateLatest;
                }

                string winnerSource = winnerRecord?.Source ?? "Unknown";
                bool? winnerIsActive = winnerRecord?.IsActive;
                DateTime? winnerDate = winnerRecord?.UpdateDate;
                string? winnerUpdateTime = winnerRecord?.UpdateTime;

                var vendorRecord = new VendorMaster
                {
                    Id = 0,
                    SupplierName = rollupName,
                    Source = winnerSource,
                    UpdatedDateTime = winnerDate ?? DateTime.MinValue,
                    WinnerIsActive = winnerIsActive,
                    WinnerUpdateDate = winnerDate,
                    WinnerSource = winnerSource,
                    SupplierDetails = JsonConvert.SerializeObject(subRecords)
                };

                vendorMasterList.Add(vendorRecord);
            }

            return vendorMasterList;
        }

        private static SubRecordDto ToSubRecord(string source, string sourceId, string name, string groupId, string groupDesc, bool isActive, DateTime? updateDate, string updateTime)
        {
            return new SubRecordDto
            {
                Source = source,
                SourceSystemId = sourceId,
                SupplierName = name,
                GroupId = groupId,
                GroupDescription = groupDesc,
                IsActive = isActive,
                UpdateDate = updateDate,
                UpdateTime = updateTime
            };
        }

        private static DateTime? CombineDateTime(DateTime? date, string? time)
        {
            if (!date.HasValue || string.IsNullOrWhiteSpace(time)) return null;
            return TimeSpan.TryParse(time, out var t)
                ? date.Value.Date.Add(t)
                : date;
        }

        public async Task SyncVendorMasterListAsync(List<VendorMaster> vendorMasterList, string changedBy)
        {
            await _dbClient.MergeVendorMasterRecordsAsync(vendorMasterList, changedBy);
        }

        public async Task<(bool ChangesDetected, List<VendorMaster> FinalList)> SyncAndCheckChangesAsync(
    List<VendorMaster> vendorMasterList,
    string changedBy)
        {
            var changesDetected = await _dbClient.MergeVendorMasterRecordsWithFlagAsync(vendorMasterList, changedBy);
            return (changesDetected, vendorMasterList);
        }


    }
}
