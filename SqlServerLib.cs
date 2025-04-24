using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Text;
using static System.Runtime.InteropServices.JavaScript.JSType;
namespace mySalesforce {
	#region SqlServerLib.ctor
	public class SqlServerLib {
		private readonly string? _connectionString;
		private readonly ILogger<SqlServerLib> _l;
		public event EventHandler<SqlEventArg> SqlEvent;
		private void RaisSqlEvent(string message, LogLevel ll, [CallerMemberName] string callerMemberName = "", [CallerLineNumber] int callerLineNumber = 0) {
			message = $"{message}:{callerMemberName}:{callerLineNumber}";
			SqlEvent?.Invoke(this, new SqlEventArg(message, ll));
		}
		public SqlServerLib(IConfiguration configuration, ILogger<SqlServerLib> logger) {
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			_connectionString = configuration.GetConnectionString("mssql");
			_l = logger ?? throw new ArgumentNullException(nameof(logger));
			if (string.IsNullOrWhiteSpace(_connectionString))
				throw new InvalidOperationException("Connection string 'mssql' is missing or empty in configuration.");
		}
		#endregion SqlServerLib.ctor
		#region Public Methods
		public DataTable GetAll_sfoTables() {
			DataTable dataTable = new DataTable();
			try {
				using (SqlConnection connection = new SqlConnection(_connectionString)) {
					string query = @"
                        SELECT name = o.name  
						FROM sys.objects o
                        JOIN sys.schemas s ON o.schema_id = s.schema_id
                        WHERE type = 'U' and s.Name= 'sfo'";
					using (SqlCommand command = new SqlCommand(query, connection)) {
						connection.Open();
						using (SqlDataAdapter adapter = new SqlDataAdapter(command)) {
							adapter.Fill(dataTable);
						}
					}
				}
			} catch (SqlException ex) {
				RaisSqlEvent($"SQL Error:{ex.Message}", LogLevel.Error);
				throw;
			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message}", LogLevel.Error);
				throw;
			}
			RaisSqlEvent($"{dataTable.Rows.Count} rows", LogLevel.Debug);
			return dataTable;
		}
		public void ExecuteNoneQuery(string script) {
			try {
				using (SqlConnection connection = new SqlConnection(_connectionString)) {
					connection.Open();// Open the connection
					using (SqlCommand command = new SqlCommand(script, connection))
						command.ExecuteNonQuery();
				}
			} catch (SqlException ex) {
				RaisSqlEvent($"SQL Error:{ex.Message}", LogLevel.Error);
			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message}", LogLevel.Error);
			}
		}
		public string GenerateCreateTableScript(DataTable schema,string schemaName, string tableName) {
			StringBuilder sql = new StringBuilder();
			// Add IF NOT EXISTS check
			
			sql.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = '{tableName}' AND s.name = '{schemaName}')");
			sql.AppendLine("BEGIN");
			sql.AppendLine($"    CREATE TABLE [{schemaName}].[{tableName}] (");

			// Build column definitions from DataRows
			for (int i = 0; i < schema.Rows.Count; i++) {
				DataRow row = schema.Rows[i];
				string name = $"[{row["Name"].ToString()}]";
				string salesforceType = row["Type"].ToString();
				int length = Convert.ToInt32(row["Length"]);
				string sqlType = mapToSqlType(salesforceType, length, name);
				string nullability = (name == "[Id]" || salesforceType == "boolean" || salesforceType == "datetime") ? "NOT NULL" : "NULL";
				string columnDefinition = $"{name} {sqlType} {nullability}";
				sql.Append($"        {columnDefinition}");
				if (i < schema.Rows.Count - 1 || schema.Columns.Contains("Id"))
					sql.Append(",");
				sql.AppendLine();
			}
			bool hasIdColumn = false;// Add primary key constraint for Id if present
			foreach (DataRow row in schema.Rows) {
				if (row["Name"].ToString() == "Id") {
					hasIdColumn = true;
					break;
				}
			}
			if (hasIdColumn) {
				sql.AppendLine($"        CONSTRAINT PK_{tableName} PRIMARY KEY (Id)");
			}
			sql.AppendLine("    );");
			sql.AppendLine("END");
			return sql.ToString();
		}

		public List<string> GetChangeEventUrls(DataTable sfoTables) {
			
			return sfoTables.AsEnumerable()
				.Select(row => $"/data/{row["name"]}ChangeEvent")
				//.Where(name => name.EndsWith("__e", StringComparison.OrdinalIgnoreCase))
				.OrderBy(name => name)
				.ToList();
		}


		private static string mapToSqlType(string salesforceType, int length, string columnName) {
			return salesforceType.ToLower() switch {
				"string" => length > 0 && length <= 8000 ? $"VARCHAR({length})" : "NVARCHAR(MAX)",
				"reference" => length > 0 && length <= 8000 ? $"VARCHAR({length})" : "NVARCHAR(MAX)",
				"picklist" => length > 0 && length <= 8000 ? $"VARCHAR({length})" : "NVARCHAR(MAX)",
				"multipicklist" => length > 0 && length <= 8000 ? $"VARCHAR({length})" : "NVARCHAR(MAX)",
				"id" => length > 0 && length <= 8000 ? $"NVARCHAR({length})" : "NVARCHAR(MAX)",
				"boolean" => "BIT",
				"int" => "INT",
				"long" => "BIGINT",
				"double" => "FLOAT",
				"currency" => "DECIMAL(18,2)",
				"date" => "DATE",
				"datetime" => "DATETIME",
				"textarea" => "TEXT",
				"url" => "VARCHAR(MAX)",
				"encryptedstring" => "VARBINARY(MAX)",
				"email" => "NVARCHAR(80)",
				"address" => "NVARCHAR(4000)",
				"phone" => "NVARCHAR(80)",
				"anytype" => "SQL_VARIANT",
				"complexvalue" => "NVARCHAR(MAX)",
				"combobox" => "NVARCHAR(255)",
				"json" => "NVARCHAR(MAX)",
				"percent" => "DECIMAL(5,2)",
				"time" => "TIME",
				"base64" => "VARCHAR(MAX)",
				"location" => "DECIMAL(9,6)",
				_ => throw new NotSupportedException($"Salesforce type {salesforceType} for column {columnName} is not supported.")
			};
		}
	}
	#endregion	Public Methods
}
public class SqlEventArg : EventArgs {
	public LogLevel LogLevel { get; }
	public string Message { get; }
	public SqlEventArg(string message, LogLevel ll) {
		LogLevel = ll;
		Message = message;
	}
}
#region Extensions
public static class SqlServerLibExtensions {
	public static void AddIdentityColumn(this DataTable table,
											string columnName = "Id",
											int seed = 1,
											int step = 1,
											bool setAsPrimary = true) {
		if (table.Columns.Contains(columnName))
			throw new ArgumentException($"Column '{columnName}' already exists in the DataTable.");
		DataColumn identityColumn = new DataColumn(columnName, typeof(int)) {
			AutoIncrement = true,
			AutoIncrementSeed = seed,
			AutoIncrementStep = step
		};
		table.Columns.Add(identityColumn);
		if (setAsPrimary) {
			table.PrimaryKey = new DataColumn[] { identityColumn };
		}
	}
	public static DataTable ExcludeRegistered(this DataTable sfObjects,
												   DataTable dtRegistered,
												   string columnName = "Name") {
		if (sfObjects == null) throw new ArgumentNullException(nameof(sfObjects));
		if (dtRegistered == null) throw new ArgumentNullException(nameof(dtRegistered));
		if (!sfObjects.Columns.Contains(columnName) || !dtRegistered.Columns.Contains(columnName))
			throw new ArgumentException($"Column '{columnName}' must exist in both DataTables.");
		var result = sfObjects.Clone();
		var sfNames = sfObjects.AsEnumerable()
								.Select(row => row.Field<string>(columnName))
								.ToHashSet();
		var registeredNames = dtRegistered.AsEnumerable()
							  .Select(row => row.Field<string>(columnName))
							  .ToHashSet();
		var onlyInSource = sfObjects.AsEnumerable()
								 .Where(row => !registeredNames.Contains(row.Field<string>(columnName)));
		var onlyInOther = dtRegistered.AsEnumerable()
							   .Where(row => !sfNames.Contains(row.Field<string>(columnName)));

		foreach (var row in onlyInSource.Concat(onlyInOther)) {
			result.ImportRow(row);
		}
		return result;
	}
	public static void ImportAllRowsFrom(this DataTable target, DataTable source) {
		foreach (DataRow row in source.Rows) target.ImportRow(row);
	}

	public static string GetSqlDataType(DataColumn column) {
		string sqlType = column.DataType switch {
			Type t when t == typeof(string) => $"NVARCHAR({(column.MaxLength > 0 ? column.MaxLength.ToString() : "MAX")})",
			Type t when t == typeof(int) => "INT",
			Type t when t == typeof(long) => "BIGINT",
			Type t when t == typeof(short) => "SMALLINT",
			Type t when t == typeof(byte) => "TINYINT",
			Type t when t == typeof(bool) => "BIT",
			Type t when t == typeof(DateTime) => "DATETIME",
			Type t when t == typeof(decimal) => "DECIMAL(18,2)",
			Type t when t == typeof(double) => "FLOAT",
			Type t when t == typeof(float) => "REAL",
			Type t when t == typeof(Guid) => "UNIQUEIDENTIFIER",
			_ => "NVARCHAR(MAX)"
		};

		if (!column.AllowDBNull)
			sqlType += " NOT NULL";

		return sqlType;
	}
	public static string GenerateDDL(this DataSet dataSet) {
		StringBuilder ddl = new StringBuilder();

		// Drop tables if they exist (in reverse order to avoid FK conflicts)
		for (int i = dataSet.Tables.Count - 1; i >= 0; i--) {
			var table = dataSet.Tables[i];
			ddl.AppendLine($"IF OBJECT_ID('{table.TableName}', 'U') IS NOT NULL DROP TABLE {table.TableName};");
		}

		// Create tables
		foreach (DataTable table in dataSet.Tables) {
			ddl.AppendLine($"CREATE TABLE {table.TableName} (");

			// Columns
			for (int i = 0; i < table.Columns.Count; i++) {
				var column = table.Columns[i];
				string columnDef = $"    {column.ColumnName} {GetSqlDataType(column)}";

				// Handle defaults
				if (column.DefaultValue != DBNull.Value && column.DefaultValue != null) {
					string defaultValue = FormatDefaultValue(column);
					columnDef += $" DEFAULT {defaultValue}";
				}

				// Handle nullability
				if (!column.AllowDBNull && !IsPrimaryKey(column, table)) {
					columnDef += " NOT NULL";
				}

				if (i < table.Columns.Count - 1)
					columnDef += ",";

				ddl.AppendLine(columnDef);
			}

			// Primary Key
			if (table.PrimaryKey.Length > 0) {
				var pkColumns = string.Join(", ", Array.ConvertAll(table.PrimaryKey, c => c.ColumnName));
				ddl.AppendLine($"    CONSTRAINT PK_{table.TableName} PRIMARY KEY ({pkColumns})");
			}

			ddl.AppendLine(");");
		}

		// Foreign Keys
		foreach (DataRelation relation in dataSet.Relations) {
			var parentTable = relation.ParentTable.TableName;
			var childTable = relation.ChildTable.TableName;
			var parentColumn = relation.ParentColumns[0].ColumnName;
			var childColumn = relation.ChildColumns[0].ColumnName;

			ddl.AppendLine($"ALTER TABLE {childTable}");
			ddl.AppendLine($"ADD CONSTRAINT FK_{childTable}_{parentTable} FOREIGN KEY ({childColumn})");
			ddl.AppendLine($"REFERENCES {parentTable} ({parentColumn});");
		}

		return ddl.ToString();
	}

	static string FormatDefaultValue(DataColumn column) {
		if (column.DefaultValue == null)
			throw new InvalidOperationException("DefaultValue cannot be null.");

		if (column.DataType == typeof(string))
			return $"'{column.DefaultValue}'";
		if (column.DataType == typeof(DateTime))
			return "'2023-01-01'"; // Simplified for example  
		if (column.DataType == typeof(decimal) || column.DataType == typeof(int))
			return column.DefaultValue.ToString()!;
		if (column.DataType == typeof(bool))
			return (bool)column.DefaultValue ? "1" : "0";
		return "''";
	}
	static bool IsPrimaryKey(DataColumn column, DataTable table) {
		return Array.Exists(table.PrimaryKey, pk => pk.ColumnName == column.ColumnName);
	}
}
#endregion Extensions
