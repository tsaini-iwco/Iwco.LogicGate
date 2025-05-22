using Iwco.LogicGate.Connections;  // ✅ Import ConnectionStrings
using System;
using System.Data;
using System.Data.SqlClient;
using System.Text.Json.Nodes;

namespace Iwco.LogicGate.Data
{
    public class LogicGateDatabase
    {
        private readonly string _sqlConnectionString;
        private readonly Action<string>? _progress;

        public LogicGateDatabase(ConnectionStrings connectionStrings, Action<string>? progress = null)
        {
            // ✅ Use GetConnectionString to dynamically fetch the connection string
            _sqlConnectionString = connectionStrings.GetConnectionString("preprod", "datalake-raw", out _);
            _progress = progress ?? Console.WriteLine;
        }

        /// <summary>
        /// Takes a JSON root object from LogicGate, parses each record, 
        /// and stores them in [dbo].[lg_records] (auto-creating the table if needed).
        /// </summary>
        public void CopyRecordsToSql(JsonArray records)
        {
            _progress?.Invoke($"Preparing to insert {records.Count} records into [dbo].[lg_records]...");

            if (records is null || records.Count == 0)
            {
                _progress?.Invoke("No records to copy (empty JsonArray).");
                return;
            }

            // 1) Build a DataTable with columns matching our desired [lg_records] schema
            var dt = CreateLgRecordsDataTable();

            // 2) Loop through records and parse each into a DataRow
            foreach (var recordNode in records)
            {
                if (recordNode is JsonObject record)
                {
                    var row = dt.NewRow();

                    // Parse known columns
                    row["id"] = record["id"]?.ToString() ?? "";
                    row["recordName"] = record["recordName"]?.ToString() ?? "";
                    row["name"] = record["name"]?.ToString() ?? "";
                    row["status"] = record["status"]?.ToString() ?? "";
                    row["sequenceNumber"] = SafeToInt(record["sequenceNumber"]?.ToString());

                    // JSON columns (store them as text):
                    row["dates"] = record["dates"]?.ToJsonString();
                    row["assignee"] = record["assignee"]?.ToJsonString();
                    row["creator"] = record["creator"]?.ToJsonString();
                    row["application"] = record["application"]?.ToJsonString();
                    row["workflow"] = record["workflow"]?.ToJsonString();
                    row["currentStep"] = record["currentStep"]?.ToJsonString();
                    row["originStep"] = record["originStep"]?.ToJsonString();
                    row["fields"] = record["fields"]?.ToJsonString();

                    // "object" as a text column 
                    row["object"] = record["object"]?.ToString() ?? "";

                    // Catch-all column with the entire record's JSON
                    row["fullRecordJson"] = record.ToJsonString();

                    dt.Rows.Add(row);
                }
            }

            // 3) Bulk Insert into SQL (auto-create the table if it doesn't exist)
            BulkInsertDataTable(dt, "lg_records");

            _progress?.Invoke($"Inserted {dt.Rows.Count} records into [dbo].[lg_records]");
        }


        private DataTable CreateLgRecordsDataTable()
        {
            var dt = new DataTable("lg_records");

            // Basic columns
            dt.Columns.Add("id", typeof(string));
            dt.Columns.Add("recordName", typeof(string));
            dt.Columns.Add("name", typeof(string));
            dt.Columns.Add("status", typeof(string));
            dt.Columns.Add("sequenceNumber", typeof(int));

            // JSON columns (store as string for now)
            dt.Columns.Add("dates", typeof(string));
            dt.Columns.Add("assignee", typeof(string));
            dt.Columns.Add("creator", typeof(string));
            dt.Columns.Add("application", typeof(string));
            dt.Columns.Add("workflow", typeof(string));
            dt.Columns.Add("currentStep", typeof(string));
            dt.Columns.Add("originStep", typeof(string));
            dt.Columns.Add("fields", typeof(string));

            // Additional columns
            dt.Columns.Add("object", typeof(string));
            dt.Columns.Add("fullRecordJson", typeof(string));

            return dt;
        }

        private void BulkInsertDataTable(DataTable dt, string tableName)
        {
            using var conn = new SqlConnection(_sqlConnectionString);
            conn.Open();

            // Let CopySqlTable handle the DROP/CREATE logic for auto-creating the table.
            CopySqlTable.Copy(
                conn,
                "dbo",
                dt,
                create: true,
                tableName: tableName
            );

            _progress?.Invoke($"Inserted {dt.Rows.Count} rows into {tableName}");
        }

        private int SafeToInt(string? val)
        {
            return int.TryParse(val, out int result) ? result : 0;
        }
    }
}
