using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Models.Records
{
    public static class LogicGateSupplierFieldsMapper
    {
        public static LogicGateRecordWithSupplier MapToSupplierFields(LogicGateRecord record)
        {
            var fields = record.Fields;

            var erpGroupIdDescription = GetFieldValue(fields, "1RsWsmph");
            var splitValues = erpGroupIdDescription?.Split('-', 2);

            // 🔍 Use correct ID for "Supplier Status"
            var statusText = GetFieldValue(fields, "z1yECvye");
            bool isActive = string.Equals(statusText, "Active Supplier", StringComparison.OrdinalIgnoreCase);

            return new LogicGateRecordWithSupplier
            {
                RecordId = record.Id,
                UpdatedDate = record.Dates.UpdatedDate ?? DateTime.UtcNow,
                SupplierFields = new LogicGateSupplierFields
                {
                    ErpMasterRollupName = GetFieldValue(fields, "euNeNSv8"),
                    SupplierName = GetFieldValue(fields, "lW3kZxMM"),
                    ErpGroupId = splitValues?.ElementAtOrDefault(0)?.Trim(),
                    ErpGroupDescription = splitValues?.ElementAtOrDefault(1)?.Trim(),
                    PreferredPaymentType = GetFieldValue(fields, "kTuI6p93"),
                    FinancialTier = GetFieldValue(fields, "sMsfaXF8"),
                    RenegotiatedPaymentTerms = GetFieldValue(fields, "EXkIUwq0"),
                    ErpSetup = GetFieldValue(fields, "EboIoCtC"),
                    MonarchSupplierNumber1 = GetFieldValue(fields, "MJcFbbOn"),
                    MonarchSupplierNumber2 = GetFieldValue(fields, "VI19szdJ"),
                    MonarchSupplierNumber3 = GetFieldValue(fields, "vBRYbvWN"),
                    MonarchSupplierNumber4 = GetFieldValue(fields, "iSqhzoL9"),
                    IsActive = isActive // ✅ Moved into SupplierFields
                }
            };
        }


        private static string GetFieldValue(List<LogicGateField> fields, string fieldId)
        {
            return fields.FirstOrDefault(f => f.Id == fieldId)
                         ?.Values?.FirstOrDefault()?.TextValue ?? "N/A";
        }

  



    }
}
