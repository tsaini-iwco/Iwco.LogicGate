using ServiceStack.DataAnnotations;
using ServiceStack.OrmLite;
using ServiceStack.OrmLite.SqlServer;
using ServiceStack.Logging;
using ServiceStack.OrmLite.Converters;
using ServiceStack.Script;
using ServiceStack.Text;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Data
{
    public class CopySqlTable
    {
        [Alias("Tables")]
        private record TableSchema(string TABLE_NAME);

        [Alias("Columns")]
        private record ColumnSchema(string TABLE_NAME, string COLUMN_NAME, string TYPE_NAME, int COLUMN_SIZE, int DECIMAL_DIGITS, string COLUMN_DEF, int SQL_DATA_TYPE, int SQL_DATETIME_SUB, int ORDINAL_POSITION, string IS_NULLABLE);

        #region Static Methods - Copy Schema
        public static void CopySchema(string schemaConnection, string dso, DataTable tables, Func<string, (DataTable columns, DataTable indexes)> getTableSchema, Action<string>? progress = null)
        {
            progress ??= Console.WriteLine;

            using var conn = new SqlConnection(schemaConnection);
            conn.Open();

            using var txn = conn.BeginTransaction();

            // Need to drop the tables up front to do them in the right order
            foreach (var table in new[] { "Indexes", "Columns", "Tables" })
            {
                using var cmd = new SqlCommand($"DROP TABLE IF EXISTS [{dso}].[{table}]", conn, txn);
                cmd.ExecuteNonQuery();
            }

            CopySqlTable.Copy(conn, dso, tables, txn: txn);

            bool first = true;
            int i = 0;
            foreach (DataRow row in tables.Rows)
            {
                i++;
                var table = row["TABLE_NAME"]?.ToString();
                var type = row["TABLE_TYPE"]?.ToString();
                if (type == "SYSTEM TABLE") continue;

                if (table is null) continue;

                var (columns, indexes) = getTableSchema(table);
                CopySqlTable.Copy(conn, dso, columns, first, txn: txn);
                CopySqlTable.Copy(conn, dso, indexes, first, txn: txn);
                var msg = $"Saved schema for {dso}.{table} ({i} of {tables.Rows.Count})";
                progress?.Invoke(msg);

                first = false;
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = $"DELETE FROM [{dso}].[Tables] WHERE [TABLE_TYPE] <> 'TABLE';";

                cmd.ExecuteNonQuery();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText += $"ALTER TABLE [{dso}].[Tables] ALTER COLUMN [TABLE_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ALTER COLUMN [TABLE_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ALTER COLUMN [COLUMN_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] ALTER COLUMN [TABLE_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] ALTER COLUMN [INDEX_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] ALTER COLUMN [COLUMN_NAME] varchar(255) NOT NULL;";

                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] DROP CONSTRAINT IF EXISTS [FK_TableColumn];";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] DROP CONSTRAINT IF EXISTS [FK_IndexTable];";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText += $"ALTER TABLE [{dso}].[Tables] DROP CONSTRAINT IF EXISTS [PK_Table];";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] DROP CONSTRAINT IF EXISTS [PK_Column];";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] DROP CONSTRAINT IF EXISTS [PK_Index];";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText += $"ALTER TABLE [{dso}].[Tables] ADD CONSTRAINT PK_Table PRIMARY KEY (TABLE_NAME);";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ADD CONSTRAINT PK_Column PRIMARY KEY (TABLE_NAME, COLUMN_NAME);";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] ADD CONSTRAINT PK_Index PRIMARY KEY (TABLE_NAME, INDEX_NAME, COLUMN_NAME);";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ADD CONSTRAINT FK_TableColumn FOREIGN KEY (TABLE_NAME) REFERENCES [{dso}].[Tables](TABLE_NAME);";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Indexes] ADD CONSTRAINT FK_IndexTable FOREIGN KEY (TABLE_NAME) REFERENCES [{dso}].[Tables](TABLE_NAME);";
                //cmd.CommandText += "ALTER TABLE [Indexes] ADD CONSTRAINT FK_IndexColumn FOREIGN KEY (TABLE_NAME, COLUMN_NAME) REFERENCES Columns(TABLE_NAME, COLUMN_NAME);";
                cmd.ExecuteNonQuery();
            }

            txn.Commit();
        }


        public static void CopySchema(string schemaConnection, string dso, DataTable tables, DataTable columns, Action<string>? progress = null)
        {
            progress ??= Console.WriteLine;

            using var conn = new SqlConnection(schemaConnection);
            conn.Open();

            using var txn = conn.BeginTransaction();

            // Need to drop the tables up front to do them in the right order
            foreach (var table in new[] { "Indexes", "Columns", "Tables" })
            {
                using var cmd = new SqlCommand($"DROP TABLE IF EXISTS [{dso}].[{table}]", conn, txn);
                cmd.ExecuteNonQuery();
            }

            CopySqlTable.Copy(conn, dso, tables, txn: txn);
            CopySqlTable.Copy(conn, dso, columns, txn: txn);


            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = $"DELETE FROM [{dso}].[Tables] WHERE [TABLE_TYPE] <> 'TABLE';";

                cmd.ExecuteNonQuery();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText += $"ALTER TABLE [{dso}].[Tables] ALTER COLUMN [TABLE_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ALTER COLUMN [TABLE_NAME] varchar(255) NOT NULL;";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ALTER COLUMN [COLUMN_NAME] varchar(255) NOT NULL;";

                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] DROP CONSTRAINT IF EXISTS [FK_TableColumn];";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText += $"ALTER TABLE [{dso}].[Tables] DROP CONSTRAINT IF EXISTS [PK_Table];";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] DROP CONSTRAINT IF EXISTS [PK_Column];";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText += $"ALTER TABLE [{dso}].[Tables] ADD CONSTRAINT PK_Table PRIMARY KEY (TABLE_NAME);";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ADD CONSTRAINT PK_Column PRIMARY KEY (TABLE_NAME, COLUMN_NAME);";
                cmd.CommandText += $"ALTER TABLE [{dso}].[Columns] ADD CONSTRAINT FK_TableColumn FOREIGN KEY (TABLE_NAME) REFERENCES [{dso}].[Tables](TABLE_NAME);";
                cmd.ExecuteNonQuery();
            }

            txn.Commit();
        }

        #endregion

        #region Static Methods - Copy DataTable Using Schema
        public static void CopyTableUsingSchema(string schemaConnection, SqlConnection conn, string dso, DataTable dt, bool create = true, string? tableName = null, SqlTransaction? txn = null)
        {
            Dictionary<string, int> sizes = new();
            CopyTableUsingSchema(schemaConnection, conn, dso, dt, ref sizes, create, tableName, txn);
        }

        /// <summary>
        /// Copy a DataTable using the schema definition in a database table
        /// </summary>
        public static void CopyTableUsingSchema(string schemaConnection, SqlConnection conn, string dso, DataTable dt, ref Dictionary<string, int> sizes, bool create = true, string? tableName = null, SqlTransaction? txn = null, Action<string, SqlConnection, SqlTransaction?>? afterCreate = null, bool allowChange = true, string? schemaTableName = null)
        {
            tableName ??= dt.TableName;
            schemaTableName ??= tableName;


            if (create)
            {
                sizes = new();

                var dbFactory = new OrmLiteConnectionFactory(schemaConnection, SqlServerDialect.Provider);
                using var db = dbFactory.Open();

                var model = OrmLiteConfig.GetModelMetadata(typeof(ColumnSchema));
                model.Schema = dso;

                var byname = db.Select<ColumnSchema>(x => x.TABLE_NAME == schemaTableName).ToDictionary(x => x.COLUMN_NAME, x => x);

                foreach (var col in byname)
                {
                    if (col.Value.SQL_DATA_TYPE != 12) continue;
                    sizes[col.Key] = col.Value.COLUMN_SIZE / 2;
                }

                sizes = GetMinimimumSizes(dt, sizes);

                var columns = new List<ColumnSchema>();
                foreach (DataColumn col in dt.Columns)
                {
                    if (byname.TryGetValue(col.ColumnName, out var column)) columns.Add(column);
                    else throw new Exception($"Column not found {col.ColumnName}");
                }

                var createSql = GetCreateTableSql(dso, tableName, columns, sizes);

                var sql = $"DROP TABLE IF EXISTS [{dso}].[{tableName}]; {createSql}";
                var cmd = new SqlCommand(sql, conn, txn);
                cmd.ExecuteNonQuery();

                afterCreate?.Invoke($"[{dso}].[{tableName}]", conn, txn);
            }
            else if (allowChange)
            {
                var next = GetMinimimumSizes(dt, sizes);
                var changed = new Dictionary<string, int>();
                foreach (var item in next)
                {
                    if (!sizes.TryGetValue(item.Key, out var size))
                    {
                        changed[item.Key] = item.Value;
                    }
                    else if (size != item.Value)
                    {
                        changed[item.Key] = item.Value;
                    }
                }

                sizes = next;

                if (changed.Count > 0)
                {
                    var sb = new StringBuilder();
                    foreach (var item in changed)
                    {
                        sb.AppendLine($"ALTER TABLE [{dso}].[{tableName}] ALTER COLUMN [{item.Key}] varchar({item.Value});");
                    }
                    var cmd = new SqlCommand(sb.ToString(), conn, txn);
                    cmd.ExecuteNonQuery();
                }
            }

            CopyDataTable(conn, dso, dt, tableName, txn);
        }

        private static Dictionary<string, int> GetMinimimumSizes(DataTable dt, in Dictionary<string, int>? sizes = null)
        {
            List<string> columns = new();
            foreach (DataColumn dataColumn in dt.Columns) { columns.Add(dataColumn.ColumnName); }
            var result = new Dictionary<string, int>(sizes ?? new());
            foreach (DataRow row in dt.Rows)
            {
                for (int i = 0; i < columns.Count; i++)
                {
                    if (row[i] is string s)
                    {
                        if (!result.TryGetValue(columns[i], out int size)) size = s.Length;
                        result[columns[i]] = size > s.Length ? size : s.Length;
                    }
                }
            }

            return result;
        }

        private static string GetSqlType(int sqlDataType, int columnSize, int? datetimeSubType = null, int? decimalDigits = null, int minSize = 0)
        {
            return sqlDataType switch
            {
                12 => $"VARCHAR({(columnSize / 2 < minSize ? minSize : columnSize / 2)})",
                2 => $"NUMERIC({columnSize},{decimalDigits ?? 0})",
                4 => "INTEGER",
                -5 => "BIGINT",
                -7 => "BIT",
                9 => datetimeSubType switch
                {
                    1 => "DATE",
                    2 => "TIME",
                    3 => "DATETIME",
                    _ => "",
                },
                -3 or -4 => "BLOB",
                -1 => "NVARCHAR(MAX)",

                5 => "SMALLINT",
                3 => "DECIMAL",
                8 => "FLOAT",
                7 => "REAL",
                -360 => "DECFLOAT",
                1 => "CHARACTER",
                -2 => "CHAR",
                -99 => "CLOB",
                -95 => "GRAPHIC",
                -96 => "VARGRAPHIC",
                -350 => "DBCLOB",
                -8 => "NCHAR",
                -9 => "NVARCHAR",
                -10 => "NCLOB",
                -98 => "BLOB",
                70 => "DATALINK",
                -100 => "ROWID",
                -370 => "XML",
                17 => "DISTINCT",
                _ => "",
            };
        }

        private static string GetCreateTableSql(string dso, string table, List<ColumnSchema> columns, Dictionary<string, int> sizes)
        {

            var sb = new StringBuilder();

            sb.AppendLine($"CREATE TABLE [{dso}].[{table}] (");
            var lastcol = columns[^1].COLUMN_NAME;

            foreach (var column in columns)
            {
                if (!sizes.TryGetValue(column.COLUMN_NAME, out var sz)) sz = 0;

                var ctype = GetSqlType(column.SQL_DATA_TYPE, column.COLUMN_SIZE, column.SQL_DATETIME_SUB, column.DECIMAL_DIGITS, sz);
                var cname = column.COLUMN_NAME;

                var dtype = ctype.Split('(')[0];
                var defval = (dtype, column.COLUMN_DEF) switch
                {
                    (_, "sysdate") => "DEFAULT CURRENT_TIMESTAMP",
                    ("TIMESTAMP", _) => "",
                    (_, "NULL") => "DEFAULT (NULL)",
                    ("NUMERIC" or "BIT" or "INTEGER" or "BIGINT", _) => $"DEFAULT {column.COLUMN_DEF}",

                    (_, null) => "",
                    (_, "") => "",
                    _ => $"DEFAULT '{column.COLUMN_DEF}'",
                };

                var nullable = column.IS_NULLABLE == "YES" ? "NULL" : "NOT NULL";
                sb.AppendLine($"[{cname}] {ctype} {defval} {nullable}{(cname == lastcol ? "" : ", ")}");
            }
            sb.AppendLine(");");
            return sb.ToString();
        }

        public static void CopyDataTable(SqlConnection conn, string dso, DataTable dt, string? tableName = null, SqlTransaction? txn = null)
        {
            tableName ??= dt.TableName;
            using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, txn);
            // Set timeout to 30 minutes
            bulkCopy.BulkCopyTimeout = 30 * 60;
            bulkCopy.DestinationTableName = $"[{dso}].[{tableName}]";
            DateTime start = DateTime.Now;
            bulkCopy.WriteToServer(dt);
            var delta = DateTime.Now - start;
        }

        #endregion

        #region Static Methods - Copy DataTable

        public static void Copy(SqlConnection conn, string dso, DataTable dt, bool create = true, string? tableName = null, SqlTransaction? txn = null, Dictionary<string, string>? schema = null, Action<string, SqlConnection, SqlTransaction?>? afterCreate = null)
        {
            if (create)
            {

                tableName ??= dt.TableName;
                string sql = GetCreateFromDataTableSQL(dso, tableName, dt, schema);

                sql = $"DROP TABLE IF EXISTS [{dso}].[{tableName}]; " + sql + ")";

                SqlCommand cmd = new SqlCommand(sql, conn, txn);
                cmd.ExecuteNonQuery();

                if (afterCreate is not null)
                {
                    afterCreate.Invoke(tableName, conn, txn);
                }
            }

            CopyDataTable(conn, dso, dt, tableName, txn);
        }


        public static string GetCreateFromDataTableSQL(string dso, string tableName, DataTable table, Dictionary<string, string>? schema = null)
        {

            string sql = $"CREATE TABLE [{dso}].[{tableName}] (\n";

            // columns

            foreach (DataColumn column in table.Columns)
            {
                var coltype = SQLGetType(column);
                if (schema is not null && schema.TryGetValue(column.ColumnName, out var ct)) coltype = ct;

                sql += "[" + column.ColumnName + "] " + coltype + ",\n";
            }

            sql = sql.TrimEnd(new char[] { ',', '\n' }) + "\n";

            // primary keys

            if (table.PrimaryKey.Length > 0)
            {

                sql += "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED (";

                foreach (DataColumn column in table.PrimaryKey)
                {

                    sql += "[" + column.ColumnName + "],";

                }

                sql = sql.TrimEnd(new char[] { ',' }) + "))\n";

            }



            return sql;

        }

        public static string[] GetPrimaryKeys(DataTable schema)
        {
            List<string> keys = new List<string>();

            foreach (DataRow column in schema.Rows)
            {
                if (schema.Columns.Contains("IsKey") && (bool)column["IsKey"])
                {
                    if (column["ColumnName"] is string name) keys.Add(name);
                }
            }

            return keys.ToArray();
        }

        // Return T-SQL data type definition, based on schema definition for a column
        public static string SQLGetType(object type, int columnSize, int numericPrecision, int numericScale)
        {

            switch (type.ToString())
            {

                case "System.String":
                    return "VARCHAR(MAX)";

                case "System.Decimal":
                    if (numericScale > 0)
                        return "REAL";
                    else if (numericPrecision > 10)
                        return "BIGINT";
                    else
                        return "INT";

                case "System.Double":
                case "System.Single":
                    return "REAL";

                case "System.Int64":
                    return "BIGINT";

                case "System.Int16":
                case "System.Int32":
                    return "INT";

                case "System.DateTime":
                    return "DATETIME";
                case "System.TimeSpan":
                    return "TIME";

                case "System.Boolean":
                    return "BIT";
                case "System.Byte[]":
                    return "VARBINARY(MAX)";
                default:

                    throw new Exception(type.ToString() + " not implemented.");

            }

        }

        // Overload based on row from schema table
        public static string SQLGetType(DataRow schemaRow)
        {
            if (schemaRow["ColumnSize"].ToString() is string sz &&
                schemaRow["NumericPrecision"].ToString() is string precision &&
                schemaRow["NumericScale"].ToString() is string scale)
            {
                return SQLGetType(schemaRow["DataType"],
                                 int.Parse(sz),
                                 int.Parse(precision),
                                 int.Parse(scale));
            }

            return "";
        }

        // Overload based on DataColumn from DataTable type
        public static string SQLGetType(DataColumn column)
        {

            return SQLGetType(column.DataType, column.MaxLength, 10, 2);

        }

        #endregion
    }
}
