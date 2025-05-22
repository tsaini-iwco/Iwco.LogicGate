using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Iwco.LogicGate.Models.Records;
using Iwco.LogicGate.Connections; 
using Newtonsoft.Json;

namespace Iwco.LogicGate.Data.DbClients
{
    public class VendorMasterDbClient
    {
        private readonly string _connectionString;

        public VendorMasterDbClient(ConnectionStrings connectionStrings)
        {
            // Fetch connection string dynamically
            _connectionString = connectionStrings.GetConnectionString("preprod", "datalake-raw", out _);
        }

        public async Task MergeVendorMasterRecordsAsync(List<VendorMaster> vendorMasters, string changedBy)
        {
            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand("dbo.p_MergeVendorMasterByTable", connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            var table = new DataTable();
            table.Columns.Add("VendorMasterId", typeof(long));
            table.Columns.Add("SupplierName", typeof(string));
            table.Columns.Add("VendorStatus", typeof(string));
            table.Columns.Add("StatusChangedDate", typeof(DateTime));
            table.Columns.Add("IsDeleted", typeof(bool));
            table.Columns.Add("IsActive", typeof(bool));
            table.Columns.Add("SupplierDetails", typeof(string));
            table.Columns.Add("SourceSystem", typeof(string));
            table.Columns.Add("VersionNo", typeof(int));
            table.Columns.Add("ChangedDate", typeof(DateTime));
            table.Columns.Add("ChangedBy", typeof(string));
            table.Columns.Add("Revision", typeof(long));
            
            foreach (var vm in vendorMasters)
            {
                var row = table.NewRow();
                row["VendorMasterId"] = DBNull.Value;
                row["SupplierName"] = string.IsNullOrWhiteSpace(vm.SupplierName) ? DBNull.Value : vm.SupplierName;

                bool isActiveVal = vm.WinnerIsActive ?? false;
                row["VendorStatus"] = isActiveVal ? "Active" : "Inactive";
                row["StatusChangedDate"] = vm.WinnerUpdateDate ?? (object)DBNull.Value;
                row["IsDeleted"] = false;
                row["IsActive"] = isActiveVal;
                row["SupplierDetails"] = string.IsNullOrWhiteSpace(vm.SupplierDetails) ? DBNull.Value : vm.SupplierDetails;
                row["SourceSystem"] = string.IsNullOrWhiteSpace(vm.Source) ? DBNull.Value : vm.Source;
                row["VersionNo"] = 1;
                row["ChangedDate"] = DateTime.UtcNow;
                row["ChangedBy"] = string.IsNullOrWhiteSpace(changedBy) ? DBNull.Value : changedBy;
                row["Revision"] = 0;

                table.Rows.Add(row);
            }


            var param = new SqlParameter("@VendorMasterTable", SqlDbType.Structured)
            {
                TypeName = "dbo.MergeVendorMasterTableType",
                Value = table
            };

            command.Parameters.Add(param);
            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var supplierName = reader["SupplierName"]?.ToString();
                var fieldChanged = reader["FieldChanged"]?.ToString();
                var oldValue = reader["OldValue"]?.ToString();
                var newValue = reader["NewValue"]?.ToString();

                Console.WriteLine($"🔄 {supplierName}: {fieldChanged} changed from '{oldValue}' to '{newValue}'");
            }

        }


        public async Task<bool> MergeVendorMasterRecordsWithFlagAsync(List<VendorMaster> vendorMasters, string changedBy)
        {
            bool anyChanges = false;

            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand("dbo.p_MergeVendorMasterByTable", connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            var table = new DataTable();
            table.Columns.Add("VendorMasterId", typeof(long));
            table.Columns.Add("SupplierName", typeof(string));
            table.Columns.Add("VendorStatus", typeof(string));
            table.Columns.Add("StatusChangedDate", typeof(DateTime));
            table.Columns.Add("IsDeleted", typeof(bool));
            table.Columns.Add("IsActive", typeof(bool));
            table.Columns.Add("SupplierDetails", typeof(string));
            table.Columns.Add("SourceSystem", typeof(string));
            table.Columns.Add("VersionNo", typeof(int));
            table.Columns.Add("ChangedDate", typeof(DateTime));
            table.Columns.Add("ChangedBy", typeof(string));
            table.Columns.Add("Revision", typeof(long));

            foreach (var vm in vendorMasters)
            {
                var row = table.NewRow();
                row["VendorMasterId"] = DBNull.Value;
                row["SupplierName"] = string.IsNullOrWhiteSpace(vm.SupplierName) ? DBNull.Value : vm.SupplierName;
                bool isActiveVal = vm.WinnerIsActive ?? false;
                row["VendorStatus"] = isActiveVal ? "Active" : "Inactive";
                row["StatusChangedDate"] = vm.WinnerUpdateDate ?? (object)DBNull.Value;
                row["IsDeleted"] = false;
                row["IsActive"] = isActiveVal;
                row["SupplierDetails"] = string.IsNullOrWhiteSpace(vm.SupplierDetails) ? DBNull.Value : vm.SupplierDetails;
                row["SourceSystem"] = string.IsNullOrWhiteSpace(vm.Source) ? DBNull.Value : vm.Source;
                row["VersionNo"] = 1;
                row["ChangedDate"] = DateTime.UtcNow;
                row["ChangedBy"] = string.IsNullOrWhiteSpace(changedBy) ? DBNull.Value : changedBy;
                row["Revision"] = 0;
                table.Rows.Add(row);
            }

            var param = new SqlParameter("@VendorMasterTable", SqlDbType.Structured)
            {
                TypeName = "dbo.MergeVendorMasterTableType",
                Value = table
            };

            command.Parameters.Add(param);
            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                anyChanges = true; // 🟢 if anything is returned, it changed
                var supplierName = reader["SupplierName"]?.ToString();
                var fieldChanged = reader["FieldChanged"]?.ToString();
                var oldValue = reader["OldValue"]?.ToString();
                var newValue = reader["NewValue"]?.ToString();

                Console.WriteLine($"🔄 {supplierName}: {fieldChanged} changed from '{oldValue}' to '{newValue}'");
            }

            return anyChanges;
        }

    }
}
