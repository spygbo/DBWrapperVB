﻿using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data; 
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql;
using MySql.Data.MySqlClient; 
using DatabaseWrapper.Core;

namespace DatabaseWrapper.Mysql
{
    /// <summary>
    /// Database client for Mysql.
    /// </summary>
    public class DatabaseClient : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// The connection string used to connect to the database.
        /// </summary>
        public string ConnectionString 
        { 
            get
            {
                return _ConnectionString;
            }
            private set
            {
                _ConnectionString = value;
            }
        }

        /// <summary>
        /// Enable or disable logging of queries using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogQueries = false;

        /// <summary>
        /// Enable or disable logging of query results using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogResults = false;

        /// <summary>
        /// Method to invoke when sending a log message.
        /// </summary>
        public Action<string> Logger = null;

        /// <summary>
        /// Timestamp format.
        /// Default is yyyy-MM-dd HH:mm:ss.ffffff.
        /// </summary>
        public string TimestampFormat
        {
            get
            {
                return MysqlHelper.TimestampFormat;
            }
            set
            {
                if (String.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(TimestampFormat));
                MysqlHelper.TimestampFormat = value;
            }
        }

        /// <summary>
        /// Maximum supported statement length.
        /// </summary>
        public int MaxStatementLength
        {
            get
            {
                return _MaxStatementLength;
            }
        }

        #endregion

        #region Private-Members

        private bool _Disposed = false;
        private string _Header = "[DatabaseWrapper.Mysql] ";
        private DatabaseSettings _Settings = null;
        private string _ConnectionString = null;
        private int _MaxStatementLength = 4194304;

        private Random _Random = new Random();

        private string _CountColumnName = "__count__";
        private string _SumColumnName = "__sum__";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Create an instance of the database client.
        /// </summary>
        /// <param name="settings">Database settings.</param>
        public DatabaseClient(DatabaseSettings settings)
        {
            _Settings = settings ?? throw new ArgumentNullException(nameof(settings));
            if (_Settings.Type != DbTypes.Mysql) throw new ArgumentException("Database settings must be of type 'Mysql'.");
            _ConnectionString = MysqlHelper.ConnectionString(_Settings);

            SetMaxStatementLength();
        }

        /// <summary>
        /// Create an instance of the database client.
        /// </summary> 
        /// <param name="serverIp">The IP address or hostname of the database server.</param>
        /// <param name="serverPort">The TCP port of the database server.</param>
        /// <param name="username">The username to use when authenticating with the database server.</param>
        /// <param name="password">The password to use when authenticating with the database server.</param> 
        /// <param name="database">The name of the database with which to connect.</param>
        public DatabaseClient( 
            string serverIp,
            int serverPort,
            string username,
            string password, 
            string database)
        {
            if (String.IsNullOrEmpty(serverIp)) throw new ArgumentNullException(nameof(serverIp));
            if (serverPort < 0) throw new ArgumentOutOfRangeException(nameof(serverPort));
            if (String.IsNullOrEmpty(database)) throw new ArgumentNullException(nameof(database));

            _Settings = new DatabaseSettings(DbTypes.Mysql, serverIp, serverPort, username, password, database);
            _ConnectionString = MysqlHelper.ConnectionString(_Settings);

            SetMaxStatementLength();
        }
         
        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the client and dispose of resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// List all tables in the database.
        /// </summary>
        /// <returns>List of strings, each being a table name.</returns>
        public List<string> ListTables()
        { 
            List<string> tableNames = new List<string>(); 
            DataTable result = Query(MysqlHelper.LoadTableNamesQuery());

            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow curr in result.Rows)
                {
                    tableNames.Add(curr["Tables_in_" + _Settings.DatabaseName].ToString());
                }
            }

            return tableNames;
        }

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if exists.</returns>
        public bool TableExists(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName)); 
            return ListTables().Contains(tableName);
        }

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <returns>A list of column objects.</returns>
        public List<Column> DescribeTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
             
            List<Column> columns = new List<Column>(); 
            DataTable result = Query(MysqlHelper.LoadTableColumnsQuery(_Settings.DatabaseName, tableName));
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow currColumn in result.Rows)
                {
                    #region Process-Each-Column

                    /*
                    public bool PrimaryKey;
                    public string Name;
                    public string DataType;
                    public int? MaxLength;
                    public bool Nullable;
                    */

                    Column tempColumn = new Column();
                    
                    tempColumn.Name = currColumn["COLUMN_NAME"].ToString();

                    tempColumn.MaxLength = null;
                    if (currColumn.Table.Columns.Contains("CHARACTER_MAXIMUM_LENGTH"))
                    {
                        int maxLength = 0;
                        if (Int32.TryParse(currColumn["CHARACTER_MAXIMUM_LENGTH"].ToString(), out maxLength))
                        {
                            tempColumn.MaxLength = maxLength;
                        }
                    }

                    tempColumn.Type = Helper.DataTypeFromString(currColumn["DATA_TYPE"].ToString());

                    if (currColumn.Table.Columns.Contains("IS_NULLABLE"))
                    {
                        if (String.Compare(currColumn["IS_NULLABLE"].ToString(), "YES") == 0) tempColumn.Nullable = true;
                        else tempColumn.Nullable = false;
                    }
                    else if (currColumn.Table.Columns.Contains("IS_NOT_NULLABLE"))
                    {
                        tempColumn.Nullable = !(Convert.ToBoolean(currColumn["IS_NOT_NULLABLE"]));
                    }

                    if (currColumn["COLUMN_KEY"] != null
                        && currColumn["COLUMN_KEY"] != DBNull.Value
                        && !String.IsNullOrEmpty(currColumn["COLUMN_KEY"].ToString()))
                    {
                        if (currColumn["COLUMN_KEY"].ToString().ToLower().Equals("pri")) tempColumn.PrimaryKey = true;
                    } 

                    if (!columns.Exists(c => c.Name.Equals(tempColumn.Name)))
                    {
                        columns.Add(tempColumn);
                    }

                    #endregion
                } 
            }

            return columns; 
        }

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <returns>Dictionary where Key is table name, value is List of Column objects.</returns>
        public Dictionary<string, List<Column>> DescribeDatabase()
        { 
            DataTable result = new DataTable();
            Dictionary<string, List<Column>> ret = new Dictionary<string, List<Column>>();
            List<string> tableNames = ListTables();

            if (tableNames != null && tableNames.Count > 0)
            {
                foreach (string tableName in tableNames)
                {
                    ret.Add(tableName, DescribeTable(tableName));
                }
            }

            return ret; 
        }

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        public void CreateTable(string tableName, List<Column> columns)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (columns == null || columns.Count < 1) throw new ArgumentNullException(nameof(columns));
            Query(MysqlHelper.CreateTableQuery(tableName, columns)); 
        }

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        public void DropTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName)); 
            Query(MysqlHelper.DropTableQuery(tableName)); 
        }

        /// <summary>
        /// Retrieve the name of the primary key column from a specific table.
        /// </summary>
        /// <param name="tableName">The table of which you want the primary key.</param>
        /// <returns>A string containing the column name.</returns>
        public string GetPrimaryKeyColumn(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details = DescribeTable(tableName);
            if (details != null && details.Count > 0)
            {
                foreach (Column c in details)
                {
                    if (c.PrimaryKey) return c.Name;
                }
            }

            return null;
        }

        /// <summary>
        /// Retrieve a list of the names of columns from within a specific table.
        /// </summary>
        /// <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
        /// <returns>A list of strings containing the column names.</returns>
        public List<string> GetColumnNames(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details = DescribeTable(tableName);
            List<string> columnNames = new List<string>();

            if (details != null && details.Count > 0)
            {
                foreach (Column c in details)
                {
                    columnNames.Add(c.Name);
                }
            }

            return columnNames;
        }

        /// <summary>
        /// Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="columnName">The column containing key or unique fields where a match is desired.</param>
        /// <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
        /// <returns>A DataTable containing at most one row.</returns>
        public DataTable GetUniqueObjectById(string tableName, string columnName, object value)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Expression e = new Expression
            {
                LeftTerm = columnName,
                Operator = Operators.Equals,
                RightTerm = value.ToString()
            };

            return Select(tableName, null, 1, null, e, null);
        }

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return Query(MysqlHelper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, null));
        }

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <param name="resultOrder">Specify on which columns and in which direction results should be ordered.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, ResultOrder[] resultOrder)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return Query(MysqlHelper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, resultOrder));
        }

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            #region Variables

            string keys = "";
            string values = ""; 
            int insertedId = 0;
            string retrievalQuery = ""; 

            #endregion

            #region Build-Key-Value-Pairs

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue; 

                if (added == 0)
                {
                    #region First

                    keys += MysqlHelper.PreparedFieldName(curr.Key);
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            values += "'" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            values += curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                values += MysqlHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                values += MysqlHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        values += "null";
                    }

                    #endregion
                }
                else
                {
                    #region Subsequent

                    keys += "," + MysqlHelper.PreparedFieldName(curr.Key);
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            values += ",'" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            values += "," + curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                values += "," + MysqlHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                values += "," + MysqlHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }

                    }
                    else
                    {
                        values += ",null";
                    }

                    #endregion
                }

                added++;
            }

            #endregion

            #region Build-INSERT-Query-and-Submit
               
            DataTable result = Query(MysqlHelper.InsertQuery(tableName, keys, values));

            #endregion

            #region Post-Retrieval
             
            if (!Helper.DataTableIsNullOrEmpty(result))
            {
                bool idFound = false;

                string primaryKeyColumn = GetPrimaryKeyColumn(tableName);

                foreach (DataRow curr in result.Rows)
                {
                    if (Int32.TryParse(curr["id"].ToString(), out insertedId))
                    {
                        idFound = true;
                        break;
                    }
                }

                if (!idFound)
                {
                    result = null;
                }
                else
                {
                    retrievalQuery = "SELECT * FROM `" + tableName + "` WHERE " + primaryKeyColumn + "=" + insertedId;
                    result = Query(retrievalQuery);
                }
            }

            #endregion
             
            return result;
        }

        /// <summary>
        /// Execute an INSERT query with multiple values within a transaction.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        public void InsertMultiple(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairList == null || keyValuePairList.Count < 1) throw new ArgumentNullException(nameof(keyValuePairList));

            #region Validate-Inputs

            Dictionary<string, object> reference = keyValuePairList[0];

            if (keyValuePairList.Count > 1)
            {
                foreach (Dictionary<string, object> dict in keyValuePairList)
                {
                    if (!(reference.Count == dict.Count) || !(reference.Keys.SequenceEqual(dict.Keys)))
                    {
                        throw new ArgumentException("All supplied dictionaries must contain exactly the same keys.");
                    }
                }
            }

            #endregion

            #region Build-Keys

            string keys = "";
            int keysAdded = 0;
            foreach (KeyValuePair<string, object> curr in reference)
            {
                if (keysAdded > 0) keys += ",";
                keys += MysqlHelper.PreparedFieldName(curr.Key);
                keysAdded++;
            }

            #endregion

            #region Build-Values

            List<string> values = new List<string>();

            foreach (Dictionary<string, object> currDict in keyValuePairList)
            {
                string vals = "";
                int valsAdded = 0;

                foreach (KeyValuePair<string, object> currKvp in currDict)
                {
                    if (valsAdded > 0) vals += ",";

                    if (currKvp.Value != null)
                    {
                        if (currKvp.Value is DateTime || currKvp.Value is DateTime?)
                        {
                            vals += "'" + DbTimestamp((DateTime)currKvp.Value) + "'";
                        }
                        else if (currKvp.Value is int || currKvp.Value is long || currKvp.Value is decimal)
                        {
                            vals += currKvp.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(currKvp.Value.ToString()))
                            {
                                vals += MysqlHelper.PreparedUnicodeValue(currKvp.Value.ToString());
                            }
                            else
                            {
                                vals += MysqlHelper.PreparedStringValue(currKvp.Value.ToString());
                            }
                        }

                    }
                    else
                    {
                        vals += "null";
                    }

                    valsAdded++;
                }

                values.Add(vals);
            }

            #endregion

            #region Build-INSERT-Query-and-Submit

            Query(MysqlHelper.InsertMultipleQuery(tableName, keys, values));

            #endregion
        }

        /// <summary>
        /// Execute an UPDATE query. 
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
        public void Update(string tableName, Dictionary<string, object> keyValuePairs, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));
             
            #region Build-Key-Value-Clause

            string keyValueClause = "";
            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue; 

                if (added == 0)
                {
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            keyValueClause += MysqlHelper.PreparedFieldName(curr.Key) + "='" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            keyValueClause += MysqlHelper.PreparedFieldName(curr.Key) + "=" + curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                keyValueClause += MysqlHelper.PreparedFieldName(curr.Key) + "=" + MysqlHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                keyValueClause += MysqlHelper.PreparedFieldName(curr.Key) + "=" + MysqlHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        keyValueClause += MysqlHelper.PreparedFieldName(curr.Key) + "= null";
                    }
                }
                else
                {
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            keyValueClause += "," + MysqlHelper.PreparedFieldName(curr.Key) + "='" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            keyValueClause += "," + MysqlHelper.PreparedFieldName(curr.Key) + "=" + curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                keyValueClause += "," + MysqlHelper.PreparedFieldName(curr.Key) + "=" + MysqlHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                keyValueClause += "," + MysqlHelper.PreparedFieldName(curr.Key) + "=" + MysqlHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        keyValueClause += "," + MysqlHelper.PreparedFieldName(curr.Key) + "= null";
                    }
                }
                added++;
            }

            #endregion

            #region Build-UPDATE-Query-and-Submit

            Query(MysqlHelper.UpdateQuery(tableName, keyValueClause, filter));

            #endregion
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        public void Delete(string tableName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            Query(MysqlHelper.DeleteQuery(tableName, filter));
        }

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        public void Truncate(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            Query(MysqlHelper.TruncateQuery(tableName)); 
        }

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Query(string query)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(query);
            if (query.Length > MaxStatementLength) throw new ArgumentException("Query exceeds maximum statement length of " + MaxStatementLength + " characters.");

            DataTable result = new DataTable();

            if (LogQueries && Logger != null) Logger(_Header + "query: " + query);

            try
            {
                using (MySqlConnection conn = new MySqlConnection(_ConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand();
                    cmd.Connection = conn;
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                    cmd.CommandText = query;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                    MySqlDataAdapter sda = new MySqlDataAdapter(cmd);
                    DataSet ds = new DataSet();
                    sda.Fill(ds);
                    if (ds != null)
                    {
                        if (ds.Tables != null)
                        {
                            if (ds.Tables.Count > 0)
                            {
                                result = ds.Tables[0];
                            }
                        }
                    }

                    conn.Close();
                }

                if (LogResults && Logger != null)
                {
                    if (result != null)
                    {
                        Logger(_Header + "result: " + result.Rows.Count + " rows");
                    }
                    else
                    {
                        Logger(_Header + "result: null");
                    }
                }

                return result;
            }
            catch (Exception e)
            {
                e.Data.Add("Query", query);
                throw;
            }
        }

        /// <summary>
        /// Determine if records exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>True if records exist.</returns>
        public bool Exists(string tableName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            DataTable result = Query(MysqlHelper.ExistsQuery(tableName, filter));
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        /// <summary>
        /// Determine the number of records that exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The number of records.</returns>
        public long Count(string tableName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            DataTable result = Query(MysqlHelper.CountQuery(tableName, _CountColumnName, filter));
            if (result != null
                && result.Rows.Count > 0
                && result.Rows[0].Table.Columns.Contains(_CountColumnName)
                && result.Rows[0][_CountColumnName] != null
                && result.Rows[0][_CountColumnName] != DBNull.Value)
            {
                return Convert.ToInt64(result.Rows[0][_CountColumnName]);
            }
            return 0;
        }

        /// <summary>
        /// Determine the sum of a column for records that match the supplied filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="fieldName">The name of the field.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The sum of the specified column from the matching rows.</returns>
        public decimal Sum(string tableName, string fieldName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(fieldName)) throw new ArgumentNullException(nameof(fieldName));
            DataTable result = Query(MysqlHelper.SumQuery(tableName, fieldName, _SumColumnName, filter));
            if (result != null
                && result.Rows.Count > 0
                && result.Rows[0].Table.Columns.Contains(_SumColumnName)
                && result.Rows[0][_SumColumnName] != null
                && result.Rows[0][_SumColumnName] != DBNull.Value)
            {
                return Convert.ToDecimal(result.Rows[0][_SumColumnName]);
            }
            return 0m;
        }

        /// <summary>
        /// Create a string timestamp from the given DateTime.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>A string with formatted timestamp.</returns>
        public string Timestamp(DateTime ts)
        {
            return MysqlHelper.DbTimestamp(ts);
        }

        /// <summary>
        /// Sanitize an input string.
        /// </summary>
        /// <param name="s">The value to sanitize.</param>
        /// <returns>A sanitized string.</returns>
        public string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return s;
            return MysqlHelper.SanitizeString(s);
        }
         
        #endregion

        #region Private-Methods

        /// <summary>
        /// Dispose of the object.
        /// </summary>
        /// <param name="disposing">Disposing of resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            { 
                // placeholder
            }

            _Disposed = true;
        }
         
        private void SetMaxStatementLength()
        {
            // https://stackoverflow.com/questions/16335011/what-is-maximum-query-size-for-mysql
            DataTable dt = Query("SHOW VARIABLES LIKE 'max_allowed_packet'");
            if (dt != null 
                && dt.Rows.Count == 1 
                && dt.Columns.Contains("Value"))
            {
                _MaxStatementLength = Convert.ToInt32(dt.Rows[0]["Value"]);
            }
        }

        #endregion

        #region Public-Static-Methods

        /// <summary>
        /// Convert a DateTime to a formatted string.
        /// </summary> 
        /// <param name="ts">The timestamp.</param>
        /// <returns>A string formatted for use with the specified database.</returns>
        public static string DbTimestamp(DateTime ts)
        {
            return MysqlHelper.DbTimestamp(ts);
        }
         
        #endregion
    }
}
