Imports System
Imports System.Collections.Generic
Imports System.Collections.Concurrent
Imports System.Data
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports MySql
Imports MySql.Data.MySqlClient
Imports DatabaseWrapper.Core
Imports ExpressionTree

Namespace DatabaseWrapper.Mysql
	''' <summary>
	''' Database client for Mysql.
	''' </summary>
	Public Class DatabaseClient
		Implements IDisposable

		#Region "Public-Members"

		''' <summary>
		''' The connection string used to connect to the database.
		''' </summary>
		Public Property ConnectionString() As String
			Get
				Return _ConnectionString
			End Get
			Private Set(ByVal value As String)
				_ConnectionString = value
			End Set
		End Property

		''' <summary>
		''' Enable or disable logging of queries using the Logger(string msg) method (default: false).
		''' </summary>
		Public LogQueries As Boolean = False

		''' <summary>
		''' Enable or disable logging of query results using the Logger(string msg) method (default: false).
		''' </summary>
		Public LogResults As Boolean = False

		''' <summary>
		''' Method to invoke when sending a log message.
		''' </summary>
		Public Logger As Action(Of String) = Nothing

		''' <summary>
		''' Timestamp format.
		''' Default is yyyy-MM-dd HH:mm:ss.ffffff.
		''' </summary>
		Public Property TimestampFormat() As String
			Get
				Return MysqlHelper.TimestampFormat
			End Get
			Set(ByVal value As String)
				If String.IsNullOrEmpty(value) Then
					Throw New ArgumentNullException(NameOf(TimestampFormat))
				End If
				MysqlHelper.TimestampFormat = value
			End Set
		End Property

		''' <summary>
		''' Timestamp format with offset.
		''' Default is yyyy-MM-dd HH:mm:ss.ffffffzzz.
		''' </summary>
		Public Property TimestampOffsetFormat() As String
			Get
				Return MysqlHelper.TimestampOffsetFormat
			End Get
			Set(ByVal value As String)
				If String.IsNullOrEmpty(value) Then
					Throw New ArgumentNullException(NameOf(TimestampOffsetFormat))
				End If
				MysqlHelper.TimestampOffsetFormat = value
			End Set
		End Property

		''' <summary>
		''' Maximum supported statement length.
		''' </summary>
		Public ReadOnly Property MaxStatementLength() As Integer
			Get
				Return _MaxStatementLength
			End Get
		End Property

		#End Region

		#Region "Private-Members"

		Private _Disposed As Boolean = False
		Private _Header As String = "[DatabaseWrapper.Mysql] "
		Private _Settings As DatabaseSettings = Nothing
		Private _ConnectionString As String = Nothing
		Private _MaxStatementLength As Integer = 4194304

		Private _Random As New Random()

		Private _CountColumnName As String = "__count__"
		Private _SumColumnName As String = "__sum__"

		#End Region

		#Region "Constructors-and-Factories"

		''' <summary>
		''' Create an instance of the database client.
		''' </summary>
		''' <param name="settings">Database settings.</param>
		Public Sub New(ByVal settings As DatabaseSettings)
			' TODO TASK: Throw expressions are not converted by :
			'ORIGINAL LINE: _Settings = settings ?? throw new ArgumentNullException(nameof(settings));
			'_Settings = If(settings, throw New ArgumentNullException(NameOf(settings)))
			If settings Is Nothing Then
				Throw New ArgumentNullException(NameOf(settings))
			Else
				_Settings = settings
			End If

			If _Settings.Type <> DbTypes.Mysql Then
				Throw New ArgumentException("Database settings must be of type 'Mysql'.")
			End If
			_ConnectionString = MysqlHelper.ConnectionString(_Settings)

			SetMaxStatementLength()
		End Sub

		''' <summary>
		''' Create an instance of the database client.
		''' </summary> 
		''' <param name="serverIp">The IP address or hostname of the database server.</param>
		''' <param name="serverPort">The TCP port of the database server.</param>
		''' <param name="username">The username to use when authenticating with the database server.</param>
		''' <param name="password">The password to use when authenticating with the database server.</param> 
		''' <param name="database">The name of the database with which to connect.</param>
		Public Sub New(ByVal serverIp As String, ByVal serverPort As Integer, ByVal username As String, ByVal password As String, ByVal database As String)
			If String.IsNullOrEmpty(serverIp) Then
				Throw New ArgumentNullException(NameOf(serverIp))
			End If
			If serverPort < 0 Then
				Throw New ArgumentOutOfRangeException(NameOf(serverPort))
			End If
			If String.IsNullOrEmpty(database) Then
				Throw New ArgumentNullException(NameOf(database))
			End If

			_Settings = New DatabaseSettings(DbTypes.Mysql, serverIp, serverPort, username, password, database)
			_ConnectionString = MysqlHelper.ConnectionString(_Settings)

			SetMaxStatementLength()
		End Sub

#End Region

#Region "Public-Methods"

		''' <summary>
		''' Tear down the client and dispose of resources.
		''' </summary>
		Public Sub Dispose() Implements IDisposable.Dispose
			Dispose(True)
			GC.SuppressFinalize(Me)
		End Sub

		''' <summary>
		''' List all tables in the database.
		''' </summary>
		''' <returns>List of strings, each being a table name.</returns>
		Public Function ListTables() As List(Of String)
			Dim tableNames As New List(Of String)()
			Dim result As DataTable = Query(MysqlHelper.LoadTableNamesQuery())

			If result IsNot Nothing AndAlso result.Rows.Count > 0 Then
				For Each curr As DataRow In result.Rows
					tableNames.Add(curr("Tables_in_" & _Settings.DatabaseName).ToString())
				Next curr
			End If

			Return tableNames
		End Function

		''' <summary>
		''' Check if a table exists in the database.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <returns>True if exists.</returns>
		Public Function TableExists(ByVal tableName As String) As Boolean
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Return ListTables().Contains(tableName)
		End Function

		''' <summary>
		''' Show the columns and column metadata from a specific table.
		''' </summary>
		''' <param name="tableName">The table to view.</param>
		''' <returns>A list of column objects.</returns>
		Public Function DescribeTable(ByVal tableName As String) As List(Of Column)
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If

			Dim columns As New List(Of Column)()
			Dim result As DataTable = Query(MysqlHelper.LoadTableColumnsQuery(_Settings.DatabaseName, tableName))
			If result IsNot Nothing AndAlso result.Rows.Count > 0 Then
				For Each currColumn As DataRow In result.Rows
					'					#Region "Process-Each-Column"

					'                    
					'                    public bool PrimaryKey;
					'                    public string Name;
					'                    public string DataType;
					'                    public int? MaxLength;
					'                    public bool Nullable;
					'                    

					Dim tempColumn As New Column()

					tempColumn.Name = currColumn("COLUMN_NAME").ToString()

					tempColumn.MaxLength = Nothing
					If currColumn.Table.Columns.Contains("CHARACTER_MAXIMUM_LENGTH") Then
						Dim maxLength As Integer = 0
						If Int32.TryParse(currColumn("CHARACTER_MAXIMUM_LENGTH").ToString(), maxLength) Then
							tempColumn.MaxLength = maxLength
						End If
					End If

					tempColumn.Type = Helper.DataTypeFromString(currColumn("DATA_TYPE").ToString())

					If currColumn.Table.Columns.Contains("IS_NULLABLE") Then
						If String.Compare(currColumn("IS_NULLABLE").ToString(), "YES") = 0 Then
							tempColumn.Nullable = True
						Else
							tempColumn.Nullable = False
						End If
					ElseIf currColumn.Table.Columns.Contains("IS_NOT_NULLABLE") Then
						tempColumn.Nullable = Not (Convert.ToBoolean(currColumn("IS_NOT_NULLABLE")))
					End If

					If currColumn("COLUMN_KEY") IsNot Nothing AndAlso currColumn("COLUMN_KEY") IsNot DBNull.Value AndAlso Not String.IsNullOrEmpty(currColumn("COLUMN_KEY").ToString()) Then
						If currColumn("COLUMN_KEY").ToString().ToLower().Equals("pri") Then
							tempColumn.PrimaryKey = True
						End If
					End If

					If Not columns.Exists(Function(c) c.Name.Equals(tempColumn.Name)) Then
						columns.Add(tempColumn)
					End If

					'					#End Region
				Next currColumn
			End If

			Return columns
		End Function

		''' <summary>
		''' Describe each of the tables in the database.
		''' </summary>
		''' <returns>Dictionary where Key is table name, value is List of Column objects.</returns>
		Public Function DescribeDatabase() As Dictionary(Of String, List(Of Column))
			Dim result As New DataTable()
			Dim ret As New Dictionary(Of String, List(Of Column))()
			Dim tableNames As List(Of String) = ListTables()

			If tableNames IsNot Nothing AndAlso tableNames.Count > 0 Then
				For Each tableName As String In tableNames
					ret.Add(tableName, DescribeTable(tableName))
				Next tableName
			End If

			Return ret
		End Function

		''' <summary>
		''' Create a table with a specified name.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="columns">Columns.</param>
		Public Sub CreateTable(ByVal tableName As String, ByVal columns As List(Of Column))
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If columns Is Nothing OrElse columns.Count < 1 Then
				Throw New ArgumentNullException(NameOf(columns))
			End If
			Query(MysqlHelper.CreateTableQuery(tableName, columns))
		End Sub

		''' <summary>
		''' Drop the specified table.  
		''' </summary>
		''' <param name="tableName">The table to drop.</param>
		Public Sub DropTable(ByVal tableName As String)
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Query(MysqlHelper.DropTableQuery(tableName))
		End Sub

		''' <summary>
		''' Retrieve the name of the primary key column from a specific table.
		''' </summary>
		''' <param name="tableName">The table of which you want the primary key.</param>
		''' <returns>A string containing the column name.</returns>
		Public Function GetPrimaryKeyColumn(ByVal tableName As String) As String
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If

			Dim details As List(Of Column) = DescribeTable(tableName)
			If details IsNot Nothing AndAlso details.Count > 0 Then
				For Each c As Column In details
					If c.PrimaryKey Then
						Return c.Name
					End If
				Next c
			End If

			Return Nothing
		End Function

		''' <summary>
		''' Retrieve a list of the names of columns from within a specific table.
		''' </summary>
		''' <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
		''' <returns>A list of strings containing the column names.</returns>
		Public Function GetColumnNames(ByVal tableName As String) As List(Of String)
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If

			Dim details As List(Of Column) = DescribeTable(tableName)
			Dim columnNames As New List(Of String)()

			If details IsNot Nothing AndAlso details.Count > 0 Then
				For Each c As Column In details
					columnNames.Add(c.Name)
				Next c
			End If

			Return columnNames
		End Function

		''' <summary>
		''' Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
		''' </summary>
		''' <param name="tableName">The table from which you wish to SELECT.</param>
		''' <param name="columnName">The column containing key or unique fields where a match is desired.</param>
		''' <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
		''' <returns>A DataTable containing at most one row.</returns>
		Public Function GetUniqueObjectById(ByVal tableName As String, ByVal columnName As String, ByVal value As Object) As DataTable
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If String.IsNullOrEmpty(columnName) Then
				Throw New ArgumentNullException(NameOf(columnName))
			End If
			If value Is Nothing Then
				Throw New ArgumentNullException(NameOf(value))
			End If

			Dim e As New Expr With {
				.Left = columnName,
				.Operator = OperatorEnum.Equals,
				.Right = value.ToString()
			}

			Return [Select](tableName, Nothing, 1, Nothing, e, Nothing)
		End Function

		''' <summary>
		''' Execute a SELECT query.
		''' </summary>
		''' <param name="tableName">The table from which you wish to SELECT.</param>
		''' <param name="indexStart">The starting index for retrieval.</param>
		''' <param name="maxResults">The maximum number of results to retrieve.</param>
		''' <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
		''' <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
		''' <returns>A DataTable containing the results.</returns>
		Public Function [Select](ByVal tableName As String, ByVal indexStart? As Integer, ByVal maxResults? As Integer, ByVal returnFields As List(Of String), ByVal filter As Expr) As DataTable
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Return Query(MysqlHelper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, Nothing))
		End Function

		''' <summary>
		''' Execute a SELECT query.
		''' </summary>
		''' <param name="tableName">The table from which you wish to SELECT.</param>
		''' <param name="indexStart">The starting index for retrieval.</param>
		''' <param name="maxResults">The maximum number of results to retrieve.</param>
		''' <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
		''' <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
		''' <param name="resultOrder">Specify on which columns and in which direction results should be ordered.</param>
		''' <returns>A DataTable containing the results.</returns>
		Public Function [Select](ByVal tableName As String, ByVal indexStart? As Integer, ByVal maxResults? As Integer, ByVal returnFields As List(Of String), ByVal filter As Expr, ByVal resultOrder() As ResultOrder) As DataTable
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Return Query(MysqlHelper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, resultOrder))
		End Function

		''' <summary>
		''' Execute an INSERT query.
		''' </summary>
		''' <param name="tableName">The table in which you wish to INSERT.</param>
		''' <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
		''' <returns>A DataTable containing the results.</returns>
		Public Function Insert(ByVal tableName As String, ByVal keyValuePairs As Dictionary(Of String, Object)) As DataTable
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If keyValuePairs Is Nothing OrElse keyValuePairs.Count < 1 Then
				Throw New ArgumentNullException(NameOf(keyValuePairs))
			End If

			'			#Region "Build-Key-Value-Pairs"

			Dim keys As String = ""
			Dim vals As String = ""
			Dim added As Integer = 0
			'Adding a parameterised types and column names
			Dim kvpfieldtype As New Dictionary(Of String, Object)

			For Each currKvp As KeyValuePair(Of String, Object) In keyValuePairs
				If String.IsNullOrEmpty(currKvp.Key) Then
					Continue For
				End If

				If added > 0 Then
					keys &= ","
					vals &= ","
				End If

				'keys &= MysqlHelper.PreparedFieldName(currKvp.Key)
				keys &= MysqlHelper.PreparedFieldName("@" & currKvp.Key)

				'Build Parameter and corresponding Value pairs
				kvpfieldtype.Add("@" & currKvp.Key, currKvp.Value)

				If currKvp.Value IsNot Nothing Then
					If TypeOf currKvp.Value Is DateTime OrElse TypeOf currKvp.Value Is DateTime? Then
						vals &= "'" & DbTimestamp(DirectCast(currKvp.Value, DateTime)) & "'"
					ElseIf TypeOf currKvp.Value Is DateTimeOffset OrElse TypeOf currKvp.Value Is DateTimeOffset? Then
						vals &= "'" & DbTimestampOffset(DirectCast(currKvp.Value, DateTimeOffset)) & "'"
					ElseIf TypeOf currKvp.Value Is Integer OrElse TypeOf currKvp.Value Is Long OrElse TypeOf currKvp.Value Is Decimal Then
						vals &= currKvp.Value.ToString()
					ElseIf TypeOf currKvp.Value Is Byte() Then
						vals &= "x'" & BitConverter.ToString(DirectCast(currKvp.Value, Byte())).Replace("-", "") & "'"
					Else
						If IsExtendedCharacters(currKvp.Value.ToString()) Then
							vals &= MysqlHelper.PreparedUnicodeValue(currKvp.Value.ToString())
						Else
							vals &= MysqlHelper.PreparedStringValue(currKvp.Value.ToString())
						End If
					End If
				Else
					vals &= "null"
				End If

				added += 1
			Next currKvp

			'			#End Region

			'			#Region "Build-INSERT-Query-and-Submit"

			'Dim result As DataTable = Query(MysqlHelper.InsertQuery(tableName, keys, vals))
			Dim result As DataTable = Query(MysqlHelper.InsertQuery(tableName, keys, vals), keys, vals, kvpfieldtype)

			'			#End Region

			'			#Region "Post-Retrieval"

			If Not Helper.DataTableIsNullOrEmpty(result) Then
				Dim idFound As Boolean = False

				Dim primaryKeyColumn As String = GetPrimaryKeyColumn(tableName)
				Dim insertedId As Integer = 0

				For Each curr As DataRow In result.Rows
					If Int32.TryParse(curr("id").ToString(), insertedId) Then
						idFound = True
						Exit For
					End If
				Next curr

				If Not idFound Then
					result = Nothing
				Else
					Dim retrievalQuery As String = "SELECT * FROM `" & tableName & "` WHERE " & primaryKeyColumn & "=" & insertedId
					result = Query(retrievalQuery)
				End If
			End If

			'			#End Region

			Return result
		End Function

		''' <summary>
		''' Execute an INSERT query with multiple values within a transaction.
		''' </summary>
		''' <param name="tableName">The table in which you wish to INSERT.</param>
		''' <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
		Public Sub InsertMultiple(ByVal tableName As String, ByVal keyValuePairList As List(Of Dictionary(Of String, Object)))
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If keyValuePairList Is Nothing OrElse keyValuePairList.Count < 1 Then
				Throw New ArgumentNullException(NameOf(keyValuePairList))
			End If

			'			#Region "Validate-Inputs"

			Dim reference As Dictionary(Of String, Object) = keyValuePairList(0)

			If keyValuePairList.Count > 1 Then
				For Each dict As Dictionary(Of String, Object) In keyValuePairList
					If Not (reference.Count = dict.Count) OrElse Not (reference.Keys.SequenceEqual(dict.Keys)) Then
						Throw New ArgumentException("All supplied dictionaries must contain exactly the same keys.")
					End If
				Next dict
			End If

			'			#End Region

			'			#Region "Build-Keys"

			Dim keys As String = ""
			Dim keysAdded As Integer = 0
			For Each curr As KeyValuePair(Of String, Object) In reference
				If keysAdded > 0 Then
					keys &= ","
				End If
				keys &= MysqlHelper.PreparedFieldName(curr.Key)
				keysAdded += 1
			Next curr

			'			#End Region

			'			#Region "Build-Values"

			Dim values As New List(Of String)()

			For Each currDict As Dictionary(Of String, Object) In keyValuePairList
				Dim vals As String = ""
				Dim valsAdded As Integer = 0

				For Each currKvp As KeyValuePair(Of String, Object) In currDict
					If valsAdded > 0 Then
						vals &= ","
					End If

					If currKvp.Value IsNot Nothing Then
						If TypeOf currKvp.Value Is DateTime OrElse TypeOf currKvp.Value Is DateTime? Then
							vals &= "'" & DbTimestamp(DirectCast(currKvp.Value, DateTime)) & "'"
						ElseIf TypeOf currKvp.Value Is DateTimeOffset OrElse TypeOf currKvp.Value Is DateTimeOffset? Then
							vals &= "'" & DbTimestampOffset(DirectCast(currKvp.Value, DateTimeOffset)) & "'"
						ElseIf TypeOf currKvp.Value Is Integer OrElse TypeOf currKvp.Value Is Long OrElse TypeOf currKvp.Value Is Decimal Then
							vals &= currKvp.Value.ToString()
						ElseIf TypeOf currKvp.Value Is Byte() Then
							vals &= "x'" & BitConverter.ToString(DirectCast(currKvp.Value, Byte())).Replace("-", "") & "'"
						Else
							If IsExtendedCharacters(currKvp.Value.ToString()) Then
								vals &= MysqlHelper.PreparedUnicodeValue(currKvp.Value.ToString())
							Else
								vals &= MysqlHelper.PreparedStringValue(currKvp.Value.ToString())
							End If
						End If

					Else
						vals &= "null"
					End If

					valsAdded += 1
				Next currKvp

				values.Add(vals)
			Next currDict

			'			#End Region

			'			#Region "Build-INSERT-Query-and-Submit"

			Query(MysqlHelper.InsertMultipleQuery(tableName, keys, values))

			'			#End Region
		End Sub

		Private Function IsExtendedCharacters(ByVal data As String) As Boolean
			If String.IsNullOrEmpty(data) Then
				Return False
			End If
			For Each c As Char In data
				If AscW(c) > 255 Then
					Return True
				End If
			Next c
			Return False
		End Function

		''' <summary>
		''' Execute an UPDATE query. 
		''' </summary>
		''' <param name="tableName">The table in which you wish to UPDATE.</param>
		''' <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
		''' <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
		Public Sub Update(ByVal tableName As String, ByVal keyValuePairs As Dictionary(Of String, Object), ByVal filter As Expr)
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If keyValuePairs Is Nothing OrElse keyValuePairs.Count < 1 Then
				Throw New ArgumentNullException(NameOf(keyValuePairs))
			End If

			'			#Region "Build-Key-Value-Clause"

			Dim keyValueClause As String = ""
			Dim added As Integer = 0

			For Each currKvp As KeyValuePair(Of String, Object) In keyValuePairs
				If String.IsNullOrEmpty(currKvp.Key) Then
					Continue For
				End If

				If added > 0 Then
					keyValueClause &= ","
				End If

				If currKvp.Value IsNot Nothing Then
					If TypeOf currKvp.Value Is DateTime OrElse TypeOf currKvp.Value Is DateTime? Then
						keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "='" & DbTimestamp(DirectCast(currKvp.Value, DateTime)) & "'"
					ElseIf TypeOf currKvp.Value Is DateTimeOffset OrElse TypeOf currKvp.Value Is DateTimeOffset? Then
						keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "='" & DbTimestampOffset(DirectCast(currKvp.Value, DateTime)) & "'"
					ElseIf TypeOf currKvp.Value Is Integer OrElse TypeOf currKvp.Value Is Long OrElse TypeOf currKvp.Value Is Decimal Then
						keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "=" & currKvp.Value.ToString()
					ElseIf TypeOf currKvp.Value Is Byte() Then
						keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "=" & "x'" & BitConverter.ToString(DirectCast(currKvp.Value, Byte())).Replace("-", "") & "'"
					Else
						If IsExtendedCharacters(currKvp.Value.ToString()) Then
							keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "=" & MysqlHelper.PreparedUnicodeValue(currKvp.Value.ToString())
						Else
							keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "=" & MysqlHelper.PreparedStringValue(currKvp.Value.ToString())
						End If
					End If
				Else
					keyValueClause &= MysqlHelper.PreparedFieldName(currKvp.Key) & "= null"
				End If

				added += 1
			Next currKvp

			'			#End Region

			'			#Region "Build-UPDATE-Query-and-Submit"

			Query(MysqlHelper.UpdateQuery(tableName, keyValueClause, filter))

			'			#End Region
		End Sub

		''' <summary>
		''' Execute a DELETE query.
		''' </summary>
		''' <param name="tableName">The table in which you wish to DELETE.</param>
		''' <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
		Public Sub Delete(ByVal tableName As String, ByVal filter As Expr)
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If filter Is Nothing Then
				Throw New ArgumentNullException(NameOf(filter))
			End If
			Query(MysqlHelper.DeleteQuery(tableName, filter))
		End Sub

		''' <summary>
		''' Empties a table completely.
		''' </summary>
		''' <param name="tableName">The table you wish to TRUNCATE.</param>
		Public Sub Truncate(ByVal tableName As String)
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Query(MysqlHelper.TruncateQuery(tableName))
		End Sub

		''' <summary>
		''' Execute a query.
		''' </summary>
		''' <param name="query_Conflict">Database query defined outside of the database client.</param>
		''' <returns>A DataTable containing the results.</returns>
		' NOTE: The parameter query was renamed since Visual Basic will not allow parameters with the same name as their enclosing function or property:
		'Public Function Query(ByVal query_Conflict As String) As DataTable
		Public Function Query(ByVal query_Conflict As String, Optional keys As String = "", Optional values As String = "", Optional keyValuePairs As Dictionary(Of String, Object) = Nothing, Optional wherevaluepair As Dictionary(Of String, Object) = Nothing) As DataTable
			If String.IsNullOrEmpty(query_Conflict) Then
				Throw New ArgumentNullException(query_Conflict)
			End If
			If query_Conflict.Length > MaxStatementLength Then
				Throw New ArgumentException("Query exceeds maximum statement length of " & MaxStatementLength & " characters.")
			End If

			Dim result As New DataTable()
			Dim reader As MySqlDataReader

			If LogQueries AndAlso Logger IsNot Nothing Then
				Logger(_Header & "query: " & query_Conflict)
			End If

			Try
				If keyValuePairs Is Nothing Then
					'No Params used- Standard implementation
					Using conn As New MySqlConnection(_ConnectionString)
						conn.Open()
						Dim cmd As New MySqlCommand()
						cmd.Connection = conn
#Disable Warning BCCA2100 ' Review SQL queries for security vulnerabilities
						cmd.CommandText = query_Conflict
#Enable Warning BCCA2100 ' Review SQL queries for security vulnerabilities
						Dim sda As New MySqlDataAdapter(cmd)
						Dim ds As New DataSet()
						sda.Fill(ds)
						If ds IsNot Nothing Then
							If ds.Tables IsNot Nothing Then
								If ds.Tables.Count > 0 Then
									result = ds.Tables(0)
								End If
							End If
						End If

						conn.Close()
					End Using
				Else
					'----------------------------------------------------------------------
					'Parametarisation begins here
					Dim DbConn As MySqlConnection
					Dim CmdSql As MySqlCommand

					'Dim sSQL1 As String = ""
					'Dim sSQL2 As String = ""
					DbConn = New MySqlConnection(_ConnectionString)

					CmdSql = New MySqlCommand(query_Conflict, DbConn)
					With CmdSql

						'For Each kval As String In arrkeys
						'    For Each vval As String In arrvalues
						If keyValuePairs IsNot Nothing Then
							For Each curr As KeyValuePair(Of String, Object) In keyValuePairs

								If curr.Value IsNot Nothing Then
									'If curr.Key.ToString = "@wallet" Then
									'    Debug.WriteLine("wallet found")
									'End If

									If TypeOf curr.Value Is Byte() Then

										.Parameters.AddWithValue(curr.Key.ToString, (curr.Value))
									ElseIf TypeOf curr.Value Is Boolean Then
										.Parameters.AddWithValue(curr.Key.ToString, Convert.ToBoolean(curr.Value))
									ElseIf TypeOf curr.Value Is String Then

										.Parameters.AddWithValue(curr.Key.ToString, Convert.ToString(curr.Value))
									ElseIf TypeOf curr.Value Is Integer Then
										.Parameters.AddWithValue(curr.Key.ToString, Convert.ToInt32(curr.Value))
									ElseIf TypeOf curr.Value Is DateTime Then
										.Parameters.AddWithValue(curr.Key.ToString, Convert.ToDateTime(curr.Value).ToString("yyyy-MM-dd"))
										'.Parameters.AddWithValue(curr.Key.ToString, Convert.ToString(curr.Value))
									ElseIf TypeOf curr.Value Is Double Then
										.Parameters.AddWithValue(curr.Key.ToString, Convert.ToDouble(curr.Value))
									Else
										.Parameters.AddWithValue(curr.Key.ToString, curr.Value)
									End If
								Else
									.Parameters.AddWithValue(curr.Key.ToString, DBNull.Value)
									'null

								End If

								'Dim s As String = Convert.ToDateTime(curr.Value).ToString("yyyy-MM-dd")

							Next
						End If
						.Connection.Open()
						reader = .ExecuteReader()
						If reader.HasRows = True Then

							'result = PopulateReultsTable(reader)

							result.Load(reader)
						End If
						.Connection.Close()
						.Dispose()
					End With
				End If

				If LogResults AndAlso Logger IsNot Nothing Then
					If result IsNot Nothing Then
						Logger(_Header & "result: " & result.Rows.Count & " rows")
					Else
						Logger(_Header & "result: null")
					End If
				End If

				Return result
			Catch e As Exception
				e.Data.Add("Query", query_Conflict)
				Throw


			End Try
		End Function

		''' <summary>
		''' Determine if records exist by filter.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="filter">Expression.</param>
		''' <returns>True if records exist.</returns>
		Public Function Exists(ByVal tableName As String, ByVal filter As Expr) As Boolean
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Dim result As DataTable = Query(MysqlHelper.ExistsQuery(tableName, filter))
			If result IsNot Nothing AndAlso result.Rows.Count > 0 Then
				Return True
			End If
			Return False
		End Function

		''' <summary>
		''' Determine the number of records that exist by filter.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="filter">Expression.</param>
		''' <returns>The number of records.</returns>
		Public Function Count(ByVal tableName As String, ByVal filter As Expr) As Long
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			Dim result As DataTable = Query(MysqlHelper.CountQuery(tableName, _CountColumnName, filter))
			If result IsNot Nothing AndAlso result.Rows.Count > 0 AndAlso result.Rows(0).Table.Columns.Contains(_CountColumnName) AndAlso result.Rows(0)(_CountColumnName) IsNot Nothing AndAlso result.Rows(0)(_CountColumnName) IsNot DBNull.Value Then
				Return Convert.ToInt64(result.Rows(0)(_CountColumnName))
			End If
			Return 0
		End Function

		''' <summary>
		''' Determine the sum of a column for records that match the supplied filter.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="fieldName">The name of the field.</param>
		''' <param name="filter">Expression.</param>
		''' <returns>The sum of the specified column from the matching rows.</returns>
		Public Function Sum(ByVal tableName As String, ByVal fieldName As String, ByVal filter As Expr) As Decimal
			If String.IsNullOrEmpty(tableName) Then
				Throw New ArgumentNullException(NameOf(tableName))
			End If
			If String.IsNullOrEmpty(fieldName) Then
				Throw New ArgumentNullException(NameOf(fieldName))
			End If
			Dim result As DataTable = Query(MysqlHelper.SumQuery(tableName, fieldName, _SumColumnName, filter))
			If result IsNot Nothing AndAlso result.Rows.Count > 0 AndAlso result.Rows(0).Table.Columns.Contains(_SumColumnName) AndAlso result.Rows(0)(_SumColumnName) IsNot Nothing AndAlso result.Rows(0)(_SumColumnName) IsNot DBNull.Value Then
				Return Convert.ToDecimal(result.Rows(0)(_SumColumnName))
			End If
			Return 0D
		End Function

		''' <summary>
		''' Create a string timestamp from the given DateTime.
		''' </summary>
		''' <param name="ts">DateTime.</param>
		''' <returns>A string with formatted timestamp.</returns>
		Public Function Timestamp(ByVal ts As DateTime) As String
			Return MysqlHelper.DbTimestamp(ts)
		End Function

		''' <summary>
		''' Create a string timestamp with offset from the given DateTimeOffset.
		''' </summary>
		''' <param name="ts">DateTimeOffset.</param>
		''' <returns>A string with formatted timestamp.</returns>
		Public Function TimestampOffset(ByVal ts As DateTimeOffset) As String
			Return MysqlHelper.DbTimestampOffset(ts)
		End Function

		''' <summary>
		''' Sanitize an input string.
		''' </summary>
		''' <param name="s">The value to sanitize.</param>
		''' <returns>A sanitized string.</returns>
		Public Function SanitizeString(ByVal s As String) As String
			If String.IsNullOrEmpty(s) Then
				Return s
			End If
			Return MysqlHelper.SanitizeString(s)
		End Function

		#End Region

		#Region "Private-Methods"

		''' <summary>
		''' Dispose of the object.
		''' </summary>
		''' <param name="disposing">Disposing of resources.</param>
		Protected Overridable Sub Dispose(ByVal disposing As Boolean)
			If _Disposed Then
				Return
			End If

			If disposing Then
				' placeholder
			End If

			_Disposed = True
		End Sub

		Private Sub SetMaxStatementLength()
			' https://stackoverflow.com/questions/16335011/what-is-maximum-query-size-for-mysql
			Dim dt As DataTable = Query("SHOW VARIABLES LIKE 'max_allowed_packet'")
			If dt IsNot Nothing AndAlso dt.Rows.Count = 1 AndAlso dt.Columns.Contains("Value") Then
				_MaxStatementLength = Convert.ToInt32(dt.Rows(0)("Value"))
			End If
		End Sub

		#End Region

		#Region "Public-Static-Methods"

		''' <summary>
		''' Convert a DateTime to a formatted string.
		''' </summary> 
		''' <param name="ts">The timestamp.</param>
		''' <returns>A string formatted for use with the specified database.</returns>
		Public Shared Function DbTimestamp(ByVal ts As DateTime) As String
			Return MysqlHelper.DbTimestamp(ts)
		End Function

		''' <summary>
		''' Convert a DateTimeOffset to a formatted string.
		''' </summary> 
		''' <param name="ts">The timestamp.</param>
		''' <returns>A string formatted for use with the specified database.</returns>
		Public Shared Function DbTimestampOffset(ByVal ts As DateTimeOffset) As String
			Return MysqlHelper.DbTimestampOffset(ts)
		End Function

		#End Region
	End Class
End Namespace
