using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using static System.Runtime.InteropServices.JavaScript.JSType;
namespace mySalesforce {
	#region SqlServerLib.ctor
	#region enums
	public enum SqlEvents {
		None,
		Created,
		Inserted,
		Deleted,
		Updated,
		ReSeeded,
		SqlException,
		Exception,
	}
	#endregion enums
	#region event args
	public class SqlEventArg : EventArgs {
		public LogLevel LogLevel { get; }
		public string Message { get; }
		public SqlEvents SqlEvent { get; }
		public string ReturningFrom { get; }
		public bool HasErrors { get; } = false;
		public SqlEventArg(string message, SqlEvents evt, LogLevel ll, string returningFrom, bool hasErrors) {
			LogLevel = ll;
			Message = message;
			SqlEvent = evt;
			ReturningFrom = returningFrom;
			hasErrors = hasErrors;
		}
	}
	public class SqlObjectQuery : EventArgs {
		public LogLevel Loglevel { get; }
		public string ObjectName { get; }
		public string ObjectType { get; }
		public bool Exist { get; }
		public string Query { get; }
		public int Id { get; }
		public string message { get; }
		public SqlObjectQuery(string objectName, string objectType, int id, bool exist, string query, string msg) {

			ObjectName = objectName;
			ObjectType = objectType;
			Exist = exist;
			Query = query;
			message = msg;
			Loglevel = LogLevel.None;// this event is not for logging
			Id = id;// row id when exist -1 otherwise
		}
	}
	#endregion event args

	public class ColumnMetadata {
		public string ColumnName { get; set; }
		public string DataType { get; set; }
		public bool IsNullable { get; set; }
		public int MaxLength { get; set; }
	}
	public class SqlServerLib {
		private readonly string? _connectionString;
		private readonly ILogger<SqlServerLib> _l;
		private readonly string _sqlSchemaName = "sfo";
		public event EventHandler<SqlEventArg> SqlEvent;
		public event EventHandler<SqlObjectQuery> SqlObjectExist;
		private void RaisSqlEvent(string message, SqlEvents enmSqlEvent, LogLevel ll, bool hasErrors, [CallerMemberName] string callerMemberName = "", [CallerLineNumber] int callerLineNumber = 0) {
			message = $"{message}:{callerMemberName}:{callerLineNumber}";
			SqlEvent?.Invoke(this, new SqlEventArg(message, enmSqlEvent, ll, callerMemberName, hasErrors));
		}
		private void RaisSqlObjectExist(int objectId, string objectName, string objectType, bool exists, string query, [CallerMemberName] string mn = "", [CallerLineNumber] int ln = 0) {
			string msg = $"{objectName}:{objectType}:{objectId}:{exists}:{mn}:{ln}";

			SqlObjectExist?.Invoke(this, new SqlObjectQuery(objectName, objectType, objectId, exists, query, msg));
		}

		public SqlServerLib(IConfiguration configuration, ILogger<SqlServerLib> logger) {
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			_connectionString = configuration.GetConnectionString("mssql");
			_sqlSchemaName = configuration.GetSection("Salesforce")["SqlSchemaName"]!;

			_l = logger ?? throw new ArgumentNullException(nameof(logger));
			if (string.IsNullOrWhiteSpace(_connectionString))
				throw new InvalidOperationException("Connection string 'mssql' is missing or empty in configuration.");
		}
		#endregion SqlServerLib.ctor
		#region Public Methods
		public DataTable GetAll_sfoTables() /* gets all tables in sfo schema*/ {
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
				RaisSqlEvent($"SQL Error:{ex.Message}", SqlEvents.SqlException, LogLevel.Error, true);
				throw;
			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
				throw;
			}
			RaisSqlEvent($"{dataTable.Rows.Count} rows", SqlEvents.None, LogLevel.Debug, true);
			return dataTable;
		}
		public DataTable Select(string sql, string primaryKey = "Id") {
			DataTable dataTable = new DataTable();
			try {
				using (SqlConnection connection = new SqlConnection(_connectionString)) {
					connection.Open();
					using (SqlCommand command = new SqlCommand(sql, connection)) {
						using (SqlDataAdapter adapter = new SqlDataAdapter(command)) {
							adapter.Fill(dataTable);
						}
					}
				}
			} catch (SqlException ex) {
				RaisSqlEvent($"SQL Error:{ex.Message} , stmt:{sql}", SqlEvents.SqlException, LogLevel.Error, true);
				throw;
			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message} , stmt:{sql}", SqlEvents.Exception, LogLevel.Error, true);
				throw;
			}
			return dataTable;
		}
		public int ExecuteScalar(string sql) {
			int result = 0;
			try {
				using (SqlConnection connection = new SqlConnection(_connectionString)) {
					connection.Open();
					using (SqlCommand command = new SqlCommand(sql, connection)) {
						result = Convert.ToInt32(command.ExecuteScalar());
					}
				}
			} catch (SqlException ex) {
				RaisSqlEvent($"SQL Error:{ex.Message}", SqlEvents.SqlException, LogLevel.Error, true);
				throw;
			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
				throw;
			}
			return result;
		}
		public void ExecuteNoneQuery(string script) {
			try {
				using (SqlConnection connection = new SqlConnection(_connectionString)) {
					connection.Open();// Open the connection
					using (SqlCommand command = new SqlCommand(script, connection))
						command.ExecuteNonQuery();

				}
			} catch (SqlException ex) {
				//RaisSqlEvent($"SQL Error:{ex.Message}",SqlEvents.SqlException, LogLevel.Error);
				RaisSqlEvent($"SQL Error:{ex.Message}", SqlEvents.SqlException, LogLevel.Error, true);

			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
			}

		}
		public void DeleteCDCObject(string objectName) {
			try {
				ExecuteNoneQuery($"DELETE FROM CDCObjects WHERE objectName ='{objectName}'");
				RaisSqlEvent($"Deleted {objectName} from CDC", SqlEvents.Deleted, LogLevel.Information, hasErrors: false);
			} catch (SqlException ex) {
				RaisSqlEvent($"SQL Error:{ex.Message}", SqlEvents.SqlException, LogLevel.Error, true);
			} catch (Exception ex) {
				RaisSqlEvent($"Error:{ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
			}
		}
		private string queryStringsForInsert(DataTable dt) {
			var rows = dt.AsEnumerable();

			// Get column names and formatted values
			var columns = rows.Select(r => r["FieldName"].ToString()).ToList();
			var values = rows.Select(r => {
				var value = r["Value"]?.ToString();
				var dataType = r["DataType"]?.ToString();

				// Handle value formatting based on DataType using switch
				return dataType switch {
					"DateTime" when long.TryParse(value, out long unixTimestamp) =>
						// Convert Unix timestamp (milliseconds) to SQL DateTime
						$"'{DateTimeOffset.FromUnixTimeMilliseconds(unixTimestamp).UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}'",
					_ => $"'{value?.Replace("'", "''")}'" // Default case: escape single quotes
				};
			}).ToList();

			// Construct INSERT statement
			var columnsClause = string.Join(", ", columns);
			var valuesClause = string.Join(", ", values);
			return $"INSERT INTO {dt.TableName} ({columnsClause}) VALUES ({valuesClause})";
		}
		private (string _setClause, string _whereClause) queryStringsforUpdate(DataTable dt, string _keyName = "Id") {
			var rows = dt.AsEnumerable();
			var setClause = string.Join(", ", rows
				.Where(r => !string.Equals(r["FieldName"].ToString(), _keyName, StringComparison.OrdinalIgnoreCase))
				.Select(r => {
					var field = r["FieldName"].ToString();
					var value = r["Value"]?.ToString();
					var dataType = r["DataType"]?.ToString();
					value = dataType switch {   // Handle value formatting based on DataType using switch
						"DateTime" when long.TryParse(value, out long unixTimestamp) =>
							DateTimeOffset.FromUnixTimeMilliseconds(unixTimestamp)  // Convert Unix timestamp (milliseconds) to SQL DateTime
								.UtcDateTime
								.ToString("yyyy-MM-dd HH:mm:ss"),
						_ => value?.Replace("'", "''") // Default case: escape single quotes
					};
					return $"{field} = '{value}'";
				}));
			var whereValue = rows
				.FirstOrDefault(r => string.Equals(r["FieldName"].ToString(), _keyName, StringComparison.OrdinalIgnoreCase))?["Value"]?.ToString();
			var whereClause = $"{_keyName} = '{whereValue?.Replace("'", "''")}'";
			return (setClause, whereClause);
		}

		public string GenerateCreateTableScript(DataTable schema, string schemaName, string tableName) {
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
			.Select(row => {
				string name = row.Table.Columns.Contains("name") && !string.IsNullOrEmpty(row["name"]?.ToString())
					? row["name"].ToString() : row["ObjectName"].ToString();
				return $"/data/{name}ChangeEvent";
			})//.Where(name => name.EndsWith("__e", StringComparison.OrdinalIgnoreCase))
			.OrderBy(name => name)
			.ToList();
		}
		public (int RowsInserted, string TableName) RegisterExludedCDCFields(string xml) {
			if (string.IsNullOrWhiteSpace(xml))
				throw new ArgumentException("XML input cannot be empty.", nameof(xml));
			try {
				using (SqlConnection conn = new SqlConnection(_connectionString)) {
					conn.Open();
					using (SqlCommand cmd = new SqlCommand("xprRegisterCDCobject", conn)) {
						cmd.CommandType = CommandType.StoredProcedure;
						cmd.Parameters.AddWithValue("@XmlInput", xml);// Add XML input parameter
						using (SqlDataReader reader = cmd.ExecuteReader()) {    // Execute and read output
							if (reader.Read()) {
								int rowsInserted = reader.GetInt32(0); // RowsInserted
								string tableName = reader.GetString(1); // TableName
								RaisSqlEvent($"{rowsInserted} rows insert to {tableName}", SqlEvents.Inserted, LogLevel.Information, false);
								return (rowsInserted, tableName);
							}
						}
					}
				}
			} catch (SqlException ex) {
				throw new Exception($"SQL error executing xprRegisterCDCobject: {ex.Message}", ex);
			} catch (Exception ex) {
				throw new Exception($"Error processing XML: {ex.Message}", ex);
			}

			throw new Exception("No results returned from stored procedure.");
		}
		public void AssertCDCObjectExist(string objectName, string schemaName = "sfo") {
			using (SqlConnection conn = new SqlConnection(_connectionString)) {
				conn.Open();
				try {
					using (SqlCommand cmd = new SqlCommand("SELECT dbo.fnObjectid(@context,@ObjectName)", conn)) {
						cmd.Parameters.AddWithValue("@context", schemaName);
						cmd.Parameters.AddWithValue("@ObjectName", objectName);
						object result = cmd.ExecuteScalar();

						int rowNum = (result != null && result != DBNull.Value) ? Convert.ToInt32(result) : -1;

						RaisSqlObjectExist(int.Parse(rowNum.ToString()!), objectName, "Table", (rowNum > 0), cmd.CommandText);
						Console.WriteLine("Function Output: " + (rowNum));
					}
				} catch (Exception ex) {
					RaisSqlEvent($"Error executing SQL: {ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
					throw;
				}
			}

		}
		public bool AssertRecord(string ObjectName, string recordId, string schemaName = "sfo") {
			using (SqlConnection conn = new SqlConnection(_connectionString)) {
				conn.Open();
				try {
					using (SqlCommand cmd = new SqlCommand($"SELECT COUNT(*) FROM {schemaName}.{ObjectName} WHERE Id = @Id", conn)) {
						cmd.Parameters.AddWithValue("@Id", recordId);
						int count = (int)cmd.ExecuteScalar();
						return count > 0;
					}
				} catch (Exception ex) {
					RaisSqlEvent($"Error executing SQL: {ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
					throw;
				}
			}
		}
		private string columnList(DataTable dt) {
			string cList = string.Join(",", dt.Columns.Cast<DataColumn>()
			.Select(col => $"[{col.ColumnName}]"));
			return cList;
		}
		public void UpdateServerTable(DataTable modifiedTable, string schemaSelect) {
			try {
				using (SqlConnection conn = new SqlConnection(_connectionString)) {
					conn.Open();
					string columns = columnList(modifiedTable);
					SqlDataAdapter da = new SqlDataAdapter(schemaSelect, conn);
					SqlCommandBuilder cb = new SqlCommandBuilder(da);
					da.UpdateCommand = cb.GetUpdateCommand();

					// Reassign values to simulate modification
					modifiedTable.AsEnumerable()
						.Where(row => row.RowState == DataRowState.Unchanged)
						.ToList()
						.ForEach(row => {
							modifiedTable.Columns.Cast<DataColumn>()
								.ToList()
								.ForEach(col => row[col] = row[col]); // reassign same value
							row.SetModified();
						});

					var changes = modifiedTable.GetChanges(DataRowState.Modified);
					if (changes != null) {
						int rowsAffected = da.Update(changes);
						modifiedTable.AcceptChanges();
						RaisSqlEvent($"{rowsAffected} Rows Affected: Update command: {da.UpdateCommand.CommandText}", SqlEvents.Updated, LogLevel.Information, true);
					}
				}
			} catch (SqlException ex) {
				RaisSqlEvent($"SQL Error: {ex.Message}", SqlEvents.SqlException, LogLevel.Error, true);
			} catch (Exception ex) {
				RaisSqlEvent($"Error: {ex.Message}", SqlEvents.Exception, LogLevel.Error, true);
			}
		}






		/*
		public async Task InsertRecordAsync(DataTable dataTable, string schemaName = "sfo") {
			if (dataTable == null || dataTable.Rows.Count == 0) {
				throw new ArgumentException("DataTable is empty or null.");
			}
			try {
				string tableName = dataTable.TableName;
				using (var connection = new SqlConnection(_connectionString)) {
					await connection.OpenAsync();
					DataTable schemaTable = await getTableSchemaAsync(connection, tableName);// Retrieve schema using ftSfoSchema function
					DataRow row = dataTable.Rows[0];// Get the first row from DataTable
					var dtColumns = dataTable.Columns.Cast<DataColumn>()// Map DataTable columns to SQL Server schema (case-insensitive)
						.Select(col => col.ColumnName)
						.ToList();
					var validColumns = schemaTable.AsEnumerable()
						.Where(s => dtColumns.Any(dtCol => dtCol.Equals(s.Field<string>("COLUMN_NAME"), StringComparison.OrdinalIgnoreCase)))
						.Select(s => new {
							ColumnName = s.Field<string>("COLUMN_NAME"),
							DataType = s.Field<string>("DATA_TYPE"),
							IsNullable = s.Field<string>("IS_NULLABLE") == "YES",
							MaxLength = s.IsNull("CHARACTER_MAXIMUM_LENGTH") ? -1 : s.Field<int>("CHARACTER_MAXIMUM_LENGTH")
						})
						.ToList();
					if (!validColumns.Any()) throw new Exception("No matching columns found between DataTable and SQL Server table schema.");
					var columnNames = string.Join(", ", validColumns.Select(c => c.ColumnName));
					var parameterNames = string.Join(", ", validColumns.Select(c => $"@{c.ColumnName}"));
					string sql = $"INSERT INTO sfo.{tableName} ({columnNames}) VALUES ({parameterNames})";
					using (var command = new SqlCommand(sql, connection)) {
						validColumns.ForEach(col =>// Add parameters with type conversion
										{
											var dtColName = dtColumns.FirstOrDefault(c => c.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase));
											object value = row[dtColName];
											if (value == null || value == DBNull.Value) {   // Handle nullability
												if (!col.IsNullable) throw new Exception($"Column {col.ColumnName} is not nullable but received a null value.");

												command.Parameters.AddWithValue($"@{col.ColumnName}", DBNull.Value);
												return;
											}
											switch (col.DataType.ToLower()) {// Handle data type conversion and length validation
												case "varchar":
												case "nvarchar":
												string stringValue = value.ToString();
												if (col.MaxLength > 0 && stringValue.Length > col.MaxLength) throw new Exception($"Value for {col.ColumnName} exceeds maximum length of {col.MaxLength}.");
												command.Parameters.AddWithValue($"@{col.ColumnName}", stringValue);
												break;
												case "int":
												if (!int.TryParse(value.ToString(), out int intValue)) throw new Exception($"Cannot convert value for {col.ColumnName} to int.");
												command.Parameters.AddWithValue($"@{col.ColumnName}", intValue);
												break;
												case "decimal":
												if (!decimal.TryParse(value.ToString(), out decimal decimalValue)) throw new Exception($"Cannot convert value for {col.ColumnName} to decimal.");
												command.Parameters.AddWithValue($"@{col.ColumnName}", decimalValue);
												break;
												case "datetime":
												if (!DateTime.TryParse(value.ToString(), out DateTime dateValue)) throw new Exception($"Cannot convert value for {col.ColumnName} to datetime.");
												command.Parameters.AddWithValue($"@{col.ColumnName}", dateValue);
												break;
												case "bit":
												if (!bool.TryParse(value.ToString(), out bool boolValue)) throw new Exception($"Cannot convert value for {col.ColumnName} to bit.");
												command.Parameters.AddWithValue($"@{col.ColumnName}", boolValue);
												break;
												default:
												command.Parameters.AddWithValue($"@{col.ColumnName}", value.ToString());
												break;
											}
										});
						await command.ExecuteNonQueryAsync();
					}
				}
			} catch (SqlException ex) {
				throw new Exception($"SQL Server error during insert: {ex.Message}", ex);
			} catch (Exception ex) {
				throw new Exception($"Error inserting record into SQL Server: {ex.Message}", ex);
			}
		}
		*/

		public async Task UpdateRecordAsync(DataTable dataTable, string schemaName = "sfo") {
			if (dataTable == null || dataTable.Rows.Count == 0)
				throw new ArgumentException("DataTable is empty or null.");

			string tableName = dataTable.TableName;

			try {
				using (var connection = new SqlConnection(_connectionString)) {
					await connection.OpenAsync();
					DataTable schemaTable = await getTableSchemaAsync(connection, tableName);
					DataRow row = dataTable.Rows[0];
					var dtColumns = dataTable.Columns.Cast<DataColumn>()
						.Select(col => col.ColumnName).ToList();
					var validColumns = schemaTable.AsEnumerable()
						.Where(s => dtColumns.Any(dtCol => dtCol.Equals(s.Field<string>("COLUMN_NAME"), StringComparison.OrdinalIgnoreCase)))
						.Select(s => new ColumnMetadata {
							ColumnName = s.Field<string>("COLUMN_NAME"),
							DataType = s.Field<string>("DATA_TYPE"),
							IsNullable = s.Field<string>("IS_NULLABLE") == "YES",
							MaxLength = s.IsNull("CHARACTER_MAXIMUM_LENGTH") ? -1 : s.Field<int>("CHARACTER_MAXIMUM_LENGTH")
						}).ToList();
					if (!validColumns.Any()) 	
						throw new Exception("No matching columns found between DataTable and SQL Server table schema.");
					var primaryKeyColumn = validColumns.First();
					var updateAssignments = string.Join(", ", validColumns
						.Skip(1) // Skip primary key column for updates
						.Select(c => $"{c.ColumnName} = @{c.ColumnName}"));
					string sql = $"UPDATE {schemaName}.{tableName} SET {updateAssignments} WHERE {primaryKeyColumn.ColumnName} = @{primaryKeyColumn.ColumnName}";
					using (var command = new SqlCommand(sql, connection)) {
						AddParametersToCommand(command, validColumns, row, dtColumns);
						int rowsAffected = await command.ExecuteNonQueryAsync();
						if (rowsAffected == 0)
							throw new Exception("No records were updated. Record not found or data unchanged.");
					}
				}
			} catch (SqlException ex) {
				throw new Exception($"SQL Server error during update: {ex.Message}", ex);
			} catch (Exception ex) {
				throw new Exception($"Error updating record in SQL Server: {ex.Message}", ex);
			}
		}

		public async Task InsertRecordAsync(DataTable dataTable, string schemaName = "sfo") {
			if (dataTable == null || dataTable.Rows.Count == 0) throw new ArgumentException("DataTable is empty or null.");
			string tableName = dataTable.TableName;
			try {
				using (var connection = new SqlConnection(_connectionString)) {
					await connection.OpenAsync();
					DataTable schemaTable = await getTableSchemaAsync(connection, tableName);// Retrieve schema using ftSfoSchema function
					DataRow row = dataTable.Rows[0];// Get the first row from DataTable
					var dtColumns = dataTable.Columns.Cast<DataColumn>()// Map DataTable columns to SQL Server schema (case-insensitive)
						.Select(col => col.ColumnName).ToList();
					var validColumns = schemaTable.AsEnumerable()
						.Where(s => dtColumns.Any(dtCol => dtCol.Equals(s.Field<string>("COLUMN_NAME"), StringComparison.OrdinalIgnoreCase)))
						.Select(s => new ColumnMetadata {
							ColumnName = s.Field<string>("COLUMN_NAME"),
							DataType = s.Field<string>("DATA_TYPE"),
							IsNullable = s.Field<string>("IS_NULLABLE") == "YES",
							MaxLength = s.IsNull("CHARACTER_MAXIMUM_LENGTH") ? -1 : s.Field<int>("CHARACTER_MAXIMUM_LENGTH")
						}).ToList();
					if (!validColumns.Any()) throw new Exception("No matching columns found between DataTable and SQL Server table schema.");
					var columnNames = string.Join(", ", validColumns.Select(c => c.ColumnName));// Build the SQL INSERT statement
					var parameterNames = string.Join(", ", validColumns.Select(c => $"@{c.ColumnName}"));
					string sql = $"INSERT INTO {schemaName}.{tableName} ({columnNames}) VALUES ({parameterNames})";
					using (var command = new SqlCommand(sql, connection)) {
						AddParametersToCommand(command, validColumns, row, dtColumns);// Add parameters using the reusable method
						await command.ExecuteNonQueryAsync();
					}
				}
			} catch (SqlException ex) {
				throw new Exception($"SQL Server error during insert: {ex.Message}", ex);
			} catch (Exception ex) {
				throw new Exception($"Error inserting record into SQL Server: {ex.Message}", ex);
			}
		}
		#region helpers (private)
		private void AddParametersToCommand(SqlCommand command, List<ColumnMetadata> validColumns, DataRow row, List<string> dtColumns) {
			foreach (var col in validColumns) {
				// Find matching DataTable column (case-insensitive)
				var dtColName = dtColumns.FirstOrDefault(c => c.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase))
					?? throw new Exception($"Column {col.ColumnName} not found in DataTable.");

				object value = row[dtColName];

				// Handle null values
				if (value == null || value == DBNull.Value) {
					if (!col.IsNullable)
						throw new Exception($"Column {col.ColumnName} is not nullable but received a null value.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.NVarChar) { Value = DBNull.Value });
					continue;
				}

				// Handle data type conversion and validation
				switch (col.DataType.ToLower()) {
					case "varchar":
					case "nvarchar":
					string stringValue = value.ToString();
					if (col.MaxLength > 0 && stringValue.Length > col.MaxLength)
						throw new Exception($"Value for {col.ColumnName} exceeds maximum length of {col.MaxLength}.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.NVarChar, col.MaxLength) { Value = stringValue });
					break;

					case "int":
					if (!int.TryParse(value.ToString(), out int intValue))
						throw new Exception($"Cannot convert value for {col.ColumnName} to int.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.Int) { Value = intValue });
					break;

					case "bigint":
					if (!long.TryParse(value.ToString(), out long longValue))
						throw new Exception($"Cannot convert value for {col.ColumnName} to bigint.");
					if (longValue < 0)
						throw new Exception($"Unix epoch timestamp for {col.ColumnName} cannot be negative.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.BigInt) { Value = longValue });
					break;

					case "decimal":
					case "numeric":
					if (!decimal.TryParse(value.ToString(), out decimal decimalValue))
						throw new Exception($"Cannot convert value for {col.ColumnName} to decimal.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.Decimal) { Value = decimalValue });
					break;

					case "float":
					string floatString = value.ToString();
					if (string.IsNullOrEmpty(floatString)) {
						if (!col.IsNullable)
							throw new Exception($"Column {col.ColumnName} is not nullable but received an empty string.");
						command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.Float) { Value = DBNull.Value });
					} else {
						if (!double.TryParse(floatString, out double doubleValue))
							throw new Exception($"Cannot convert value for {col.ColumnName} to float.");
						command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.Float) { Value = doubleValue });
					}
					break;

					case "datetime":
					case "date":
					if (long.TryParse(value?.ToString(), out long unixMillis) && unixMillis >= 0) {
						// Convert Unix epoch milliseconds to DateTime (UTC)
						DateTime epochStart = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local);
						DateTime parsedDate = epochStart.AddMilliseconds(unixMillis);
						command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.DateTime) { Value = parsedDate });
					} else if (DateTime.TryParse(value?.ToString(), out DateTime parsedDateFromString)) {
						// Handle string-based DateTime parsing
						command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.DateTime) { Value = parsedDateFromString });
					} else {
						// Handle invalid or null values
						command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.DateTime) { Value = DBNull.Value });
					}
					break;
					case "bit":
					if (!bool.TryParse(value.ToString(), out bool boolValue))
						throw new Exception($"Cannot convert value for {col.ColumnName} to bit.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.Bit) { Value = boolValue });
					break;

					case "uniqueidentifier":
					if (!Guid.TryParse(value.ToString(), out Guid guidValue))
						throw new Exception($"Cannot convert value for {col.ColumnName} to uniqueidentifier.");
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.UniqueIdentifier) { Value = guidValue });
					break;

					default:
					command.Parameters.Add(new SqlParameter($"@{col.ColumnName}", SqlDbType.NVarChar) { Value = value.ToString() });
					break;
				}

				// Optional: Log parameter for debugging (remove in production)
				// Console.WriteLine($"Parameter @{col.ColumnName}: {command.Parameters[$"@{col.ColumnName}"].Value}");
			}
		}

		private async Task<DataTable> getTableSchemaAsync(SqlConnection connection, string tableName) {
			var schemaTable = new DataTable();
			string sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM dbo.ftSfoSchema(@TableName)";
			using (var command = new SqlCommand(sql, connection)) {
				command.Parameters.AddWithValue("@TableName", tableName);
				using (var adapter = new SqlDataAdapter(command)) adapter.Fill(schemaTable);
			}
			if (schemaTable.Rows.Count == 0) throw new Exception($"No schema found for table {tableName} in schema sfo.");
			return schemaTable;
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
		#endregion helpers (private)
	}
	#endregion	Public Methods
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
	public static string GetXml(this DataTable table, string ColumnsToSelect) {
		DataTable tblClipped = table.DefaultView.ToTable(true, ColumnsToSelect.Split(','));
		tblClipped.TableName = table.TableName;
		DataSet ds = new DataSet();
		ds.Tables.Add(tblClipped.Copy());
		ds.DataSetName = "X";
		return ds.GetXml();
	}

	public static DataTable Transpose(this DataTable inputTable) {
		if (inputTable == null || inputTable.Rows.Count == 0)
			return new DataTable();
		DataTable transposedTable = new DataTable(inputTable.TableName);
		inputTable.AsEnumerable()
			.Select(row => row["FieldName"]?.ToString())
			.Where(fieldName => !string.IsNullOrEmpty(fieldName) && !transposedTable.Columns.Contains(fieldName))
			.ToList()
			.ForEach(fieldName => transposedTable.Columns.Add(fieldName));
		DataRow newRow = transposedTable.NewRow();
		inputTable.AsEnumerable()
			.Select(row => new {
				FieldName = row["FieldName"]?.ToString(),
				FieldValue = row["Value"]?.ToString()
			})
			.Where(x => !string.IsNullOrEmpty(x.FieldName) && transposedTable.Columns.Contains(x.FieldName))
			.ToList()
			.ForEach(x => newRow[x.FieldName] = x.FieldValue);
		transposedTable.Rows.Add(newRow);

		return transposedTable;
	}

}
#endregion Extensions
