using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ClosedXML.Excel;
using Microsoft.Extensions.Logging;
using Iwco.LogicGate.Connections;   

namespace Iwco.LogicGate.Tasks.Services
{
   
    public sealed class FinanceMappingLoaderService
    {
        private const string SHEET_NAME = "Master Mapping";
        private const string DEST_TABLE = "dbo.x_FinanceMapping";

        private readonly string _connectionString;
        private readonly ILogger<FinanceMappingLoaderService> _logger;

        public FinanceMappingLoaderService(
            ConnectionStrings connectionStrings,
            ILogger<FinanceMappingLoaderService> logger)
        {
         
            _connectionString = connectionStrings.GetConnectionString("preprod", "datalake-raw", out _);
            _logger = logger;
        }

        #region Public API
        public async Task<int> LoadAsync(string excelFilePath)
        {
            if (!File.Exists(excelFilePath))
                throw new FileNotFoundException($"Excel file not found: {excelFilePath}");

            _logger.LogInformation("Loading Finance Mapping from {Path}", excelFilePath);

            //Read workbook → DataTable
            DataTable dataTable = ReadWorkbookToDataTable(excelFilePath);
            _logger.LogInformation(" Parsed {Count} rows from sheet '{Sheet}'.",
                                   dataTable.Rows.Count, SHEET_NAME);

            //TRUNCATE + BULK INSERT
            await BulkInsertAsync(dataTable);

            _logger.LogInformation(" Loaded {Count} rows into {Table}.",
                                   dataTable.Rows.Count, DEST_TABLE);

            return dataTable.Rows.Count;
        }
        #endregion

        #region Workbook helpers
        private static readonly (string header, string column)[] _columns =
        {
            ("Supplier ID",                        "SupplierID"),
            ("Supplier Name",                      "SupplierName"),
            ("Master Rollup Naming Convention",    "MasterRollupNaming"),

            ("Terms",                              "Terms"),
            ("Terms in Days",                      "TermsInDays"),
            ("Discount %",                         "DiscountPercent"),
            ("Discount/Standard",                  "DiscountOrStandard"),

            ("Supplier Group ID",                  "SupplierGroupID"),
            ("Supplier Group Description",         "SupplierGroupDescription"),
            ("Critical Supplier (Y/N)",            "CriticalSupplierYN"),

            ("Payment Type (Check, Wire, ACH)",    "PaymentType"),
            ("Source System",                      "SourceSystem"),
            ("Active (Y/N)",                       "ActiveYN"),

            ("Stretch",                            "Stretch"),
            ("Stretch Override",                   "StretchOverride"),
            ("Stretch Output",                     "StretchOutput"),

            ("2023 Spend",                         "Spend2023"),
            ("Cash Flow Grouping",                 "CashFlowGrouping"),
            ("Priority",                           "Priority"),

            ("Tier",                               "Tier"),
            ("Credit Limit",                       "CreditLimit"),
            ("Late Fees",                          "LateFees"),

            ("Notes",                              "Notes"),
            ("Historical Stretch",                 "HistoricalStretch")
        };

        private static DataTable CreateSchema()
        {
            // Build a DataTable with SQL‑friendly column types
            var dt = new DataTable();

            // string columns
            void AddStr(string name) => dt.Columns.Add(name, typeof(string));
            // numeric helpers
            void AddInt(string name) => dt.Columns.Add(name, typeof(int));
            void AddDec(string name) => dt.Columns.Add(name, typeof(decimal));

            AddStr("SupplierID");
            AddStr("SupplierName");
            AddStr("MasterRollupNaming");

            AddStr("Terms"); AddInt("TermsInDays");
            AddDec("DiscountPercent"); AddStr("DiscountOrStandard");

            AddStr("SupplierGroupID"); AddStr("SupplierGroupDescription");
            AddStr("CriticalSupplierYN");

            AddStr("PaymentType"); AddStr("SourceSystem");
            AddStr("ActiveYN");

            AddStr("Stretch"); AddStr("StretchOverride");
            AddInt("StretchOutput");

            AddDec("Spend2023"); AddStr("CashFlowGrouping");
            AddInt("Priority");

            AddStr("Tier"); AddDec("CreditLimit");
            AddDec("LateFees");

            AddStr("Notes"); AddStr("HistoricalStretch");

            return dt;
        }

        private static DataTable ReadWorkbookToDataTable(string path)
        {
            var dt = CreateSchema();

            using var workbook = new XLWorkbook(path);
            var worksheet = workbook.Worksheet(SHEET_NAME);

            var headerRow = worksheet.Row(1);
            var headerMap = headerRow.Cells()
                                           .ToDictionary(
                                               c => c.GetString().Trim(),
                                               c => c.Address.ColumnNumber,
                                               StringComparer.OrdinalIgnoreCase);

            foreach (var (header, _) in _columns)
            {
                if (!headerMap.ContainsKey(header))
                    throw new InvalidDataException(
                        $"Header '{header}' not found in '{SHEET_NAME}'.");
            }

            foreach (var row in worksheet.RangeUsed().RowsUsed().Skip(1))
            {
                if (row.IsEmpty()) continue;

                var dr = dt.NewRow();

                // String helpers
                string S(string h) => row.Cell(headerMap[h]).GetString().Trim();

                // Int helper
                object I(string h)
                {
                    var cell = row.Cell(headerMap[h]);
                    return cell.TryGetValue<int>(out var v) ? v : DBNull.Value;
                }

                // Decimal helper
                object D(string h)
                {
                    var cell = row.Cell(headerMap[h]);
                    return cell.TryGetValue<decimal>(out var v) ? v : DBNull.Value;
                }

                dr["SupplierID"] = S("Supplier ID");
                dr["SupplierName"] = S("Supplier Name");
                dr["MasterRollupNaming"] = S("Master Rollup Naming Convention");

                dr["Terms"] = S("Terms");
                dr["TermsInDays"] = I("Terms in Days");
                dr["DiscountPercent"] = D("Discount %");
                dr["DiscountOrStandard"] = S("Discount/Standard");

                dr["SupplierGroupID"] = S("Supplier Group ID");
                dr["SupplierGroupDescription"] = S("Supplier Group Description");
                dr["CriticalSupplierYN"] = S("Critical Supplier (Y/N)");

                dr["PaymentType"] = S("Payment Type (Check, Wire, ACH)");
                dr["SourceSystem"] = S("Source System");
                dr["ActiveYN"] = S("Active (Y/N)");

                dr["Stretch"] = S("Stretch");
                dr["StretchOverride"] = S("Stretch Override");
                dr["StretchOutput"] = I("Stretch Output");

                dr["Spend2023"] = D("2023 Spend");
                dr["CashFlowGrouping"] = S("Cash Flow Grouping");
                dr["Priority"] = I("Priority");

                dr["Tier"] = S("Tier");
                dr["CreditLimit"] = D("Credit Limit");
                dr["LateFees"] = D("Late Fees");

                dr["Notes"] = S("Notes");
                dr["HistoricalStretch"] = S("Historical Stretch");

                dt.Rows.Add(dr);
            }

            return dt;
        }
        #endregion

        #region Bulk‑copy helpers
        private async Task BulkInsertAsync(DataTable table)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            // 1. Wipe the table
            using (var truncate = new SqlCommand($"TRUNCATE TABLE {DEST_TABLE};", connection, transaction))
            {
                await truncate.ExecuteNonQueryAsync();
            }

            // 2. Bulk‑copy in the rows
            using (var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulk.DestinationTableName = DEST_TABLE;
                bulk.BatchSize = 5000; 
                bulk.BulkCopyTimeout = 0;    

                foreach (DataColumn col in table.Columns)
                    bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                await bulk.WriteToServerAsync(table);
            }

            transaction.Commit();
        }
        #endregion
    }
}
