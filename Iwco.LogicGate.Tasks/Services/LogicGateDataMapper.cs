using Iwco.LogicGate.Models.Records;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Tasks.Services
{
    public class LogicGateDataMapper
    {
        public Dictionary<string, MasterRollupLogicGateRecords> MapToRollups(
            List<LogicGateRecordWithSupplier> rawRecords)
        {
            var result = new Dictionary<string, MasterRollupLogicGateRecords>();

            foreach (var record in rawRecords)
            {
                string rollup = record.SupplierFields.ErpMasterRollupName?.Trim();
                if (string.IsNullOrWhiteSpace(rollup)) continue;

                if (!result.ContainsKey(rollup))
                {
                    result[rollup] = new MasterRollupLogicGateRecords
                    {
                        ErpMasterRollupName = rollup
                    };
                }

                var updated = record.UpdatedDate ?? DateTime.UtcNow;

                var supplier = new LogicGateSupplier
                {
                    SupplierId = record.RecordId,
                    SupplierName = record.SupplierFields.SupplierName,
                    GroupId = record.SupplierFields.ErpGroupId,
                    GroupDescription = record.SupplierFields.ErpGroupDescription,
                    IsActive = record.SupplierFields.IsActive,
                    UpdateDate = updated.Date,
                    UpdateTime = updated.ToString("HH:mm:ss")
                };

                result[rollup].LogicGateRecords.Add(supplier);
            }

            return result;
        }
    }

}
