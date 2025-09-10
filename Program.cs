using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.IO;
using ExcelDataReader;
using System.Text;

class Program
{
    static void Main(string[] args)
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        string excelFilePath = @"c:\VIR\BI\PL_ELABE.xlsx";
        string connectionString = $"Server={Environment.GetEnvironmentVariable("VIR_SQL_SERVER_NAME")};" +
                                  $"Database={Environment.GetEnvironmentVariable("VIR_SQL_DATABASE")};" +
                                  $"User Id={Environment.GetEnvironmentVariable("VIR_SQL_USER")};" +
                                  $"Password={Environment.GetEnvironmentVariable("VIR_SQL_PASSWORD")};" +
                                  "Connection Timeout=50000;Trust Server Certificate=true";
        string destinationTable = "Fakturownia_PL_ELABE";

        DataTable dataTable = ReadExcelFile(excelFilePath);

        if (dataTable != null)
        {
            Console.WriteLine($"Read {dataTable.Rows.Count} rows from Excel.");
            CreateSqlTableIfNotExists(dataTable, connectionString, destinationTable);
            BulkInsertIntoSql(dataTable, connectionString, destinationTable);
            Console.WriteLine("Import completed.");
        }
        else
        {
            Console.WriteLine("Failed to read data from Excel.");
        }
    }

    static DataTable ReadExcelFile(string filePath)
    {
        using var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
        using var reader = ExcelReaderFactory.CreateReader(stream);

        var config = new ExcelDataSetConfiguration
        {
            ConfigureDataTable = _ => new ExcelDataTableConfiguration
            {
                UseHeaderRow = true
            }
        };

        var result = reader.AsDataSet(config);
        return result.Tables[0]; // Read first worksheet
    }

    static void CreateSqlTableIfNotExists(DataTable table, string connectionString, string tableName)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        // Check if table exists
        var checkCmd = new SqlCommand(
            $"IF OBJECT_ID('{tableName}', 'U') IS NULL SELECT 0 ELSE SELECT 1",
            connection);
        var exists = (int)checkCmd.ExecuteScalar();

        if (exists == 1)
        {
            var truncateCmd = new SqlCommand($"TRUNCATE TABLE {tableName}", connection);
            truncateCmd.ExecuteNonQuery();
            Console.WriteLine($"Table {tableName} already exists. Skipping CREATE.");
            return;
        }

        // Build CREATE TABLE statement
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {tableName} (");

        foreach (DataColumn column in table.Columns)
        {
            string columnName = column.ColumnName.Replace(" ", "_");
            string sqlType = InferSqlType(table, column);
            sb.AppendLine($"    [{columnName}] {sqlType},");
        }

        sb.Length -= 3; // Remove trailing comma
        sb.AppendLine("\n);");

        var createCmd = new SqlCommand(sb.ToString(), connection);
        createCmd.ExecuteNonQuery();

        Console.WriteLine($"Created table {tableName}");
    }

    static string InferSqlType(DataTable table, DataColumn column)
    {
        foreach (DataRow row in table.Rows)
        {
            var val = row[column];
            if (val != DBNull.Value)
            {
                if (int.TryParse(val.ToString(), out _))
                    return "INT";
                if (double.TryParse(val.ToString(), out _))
                    return "FLOAT";
                if (DateTime.TryParse(val.ToString(), out _))
                    return "DATETIME";

                break;
            }
        }

        return "NVARCHAR(255)"; // default fallback
    }

    static void BulkInsertIntoSql(DataTable dataTable, string connectionString, string tableName)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        using var bulkCopy = new SqlBulkCopy(connection);
        bulkCopy.DestinationTableName = tableName;
        bulkCopy.BulkCopyTimeout = 300;

        foreach (DataColumn col in dataTable.Columns)
        {
            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName.Replace(" ", "_"));
        }

        bulkCopy.WriteToServer(dataTable);
    }
}
