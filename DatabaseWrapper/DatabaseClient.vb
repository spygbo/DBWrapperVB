Imports System
Imports System.Collections.Generic
Imports System.Collections.Concurrent
Imports System.Data
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports DatabaseWrapper.Core
Imports DatabaseWrapper.Mysql
Imports DatabaseWrapper.Postgresql
Imports DatabaseWrapper.Sqlite
Imports DatabaseWrapper.SqlServer
Imports ExpressionTree

Namespace DatabaseWrapper
	''' <summary>
	''' Database client for Microsoft SQL Server, Mysql, PostgreSQL, and Sqlite.
	''' </summary>
	Public Class DatabaseClient
		Implements IDisposable

		#Region "Public-Members"

		''' <summary>
		''' The type of database.
		''' </summary>
		Public ReadOnly Property Type() As DbTypes
			Get
				Return _Settings.Type
			End Get
		End Property

		''' <summary>
		''' The connection string used to connect to the database server.
		''' </summary>
		Public ReadOnly Property ConnectionString() As String
			Get
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						Return _SqlServer.ConnectionString
					Case DbTypes.Mysql
						Return _Mysql.ConnectionString
					Case DbTypes.Postgresql
						Return _Postgresql.ConnectionString
					Case DbTypes.Sqlite
						Return _Sqlite.ConnectionString
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Get
		End Property

		''' <summary>
		''' Enable or disable logging of queries using the Logger(string msg) method (default: false).
		''' </summary>
		Public Property LogQueries() As Boolean
			Get
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						Return _SqlServer.LogQueries
					Case DbTypes.Mysql
						Return _Mysql.LogQueries
					Case DbTypes.Postgresql
						Return _Postgresql.LogQueries
					Case DbTypes.Sqlite
						Return _Sqlite.LogQueries
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Get
			Set(ByVal value As Boolean)
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						_SqlServer.LogQueries = value
					Case DbTypes.Mysql
						_Mysql.LogQueries = value
					Case DbTypes.Postgresql
						_Postgresql.LogQueries = value
					Case DbTypes.Sqlite
						_Sqlite.LogQueries = value
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Set
		End Property

		''' <summary>
		''' Enable or disable logging of query results using the Logger(string msg) method (default: false).
		''' </summary>
		Public Property LogResults() As Boolean
			Get
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						Return _SqlServer.LogResults
					Case DbTypes.Mysql
						Return _Mysql.LogResults
					Case DbTypes.Postgresql
						Return _Postgresql.LogResults
					Case DbTypes.Sqlite
						Return _Sqlite.LogResults
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Get
			Set(ByVal value As Boolean)
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						_SqlServer.LogResults = value
					Case DbTypes.Mysql
						_Mysql.LogResults = value
					Case DbTypes.Postgresql
						_Postgresql.LogResults = value
					Case DbTypes.Sqlite
						_Sqlite.LogResults = value
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Set
		End Property

		''' <summary>
		''' Method to invoke when sending a log message.
		''' </summary>
		Public Property Logger() As Action(Of String)
			Get
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						Return _SqlServer.Logger
					Case DbTypes.Mysql
						Return _Mysql.Logger
					Case DbTypes.Postgresql
						Return _Postgresql.Logger
					Case DbTypes.Sqlite
						Return _Sqlite.Logger
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Get
			Set(ByVal value As Action(Of String))
				Select Case _Settings.Type
					Case DbTypes.SqlServer
						_SqlServer.Logger = value
					Case DbTypes.Mysql
						_Mysql.Logger = value
					Case DbTypes.Postgresql
						_Postgresql.Logger = value
					Case DbTypes.Sqlite
						_Sqlite.Logger = value
					Case Else
						Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End Select
			End Set
		End Property

		#End Region

		#Region "Private-Members"

		Private _Disposed As Boolean = False
		Private _Header As String = ""
		Private _Settings As DatabaseSettings = Nothing

		Private _Mysql As DatabaseWrapper.Mysql.DatabaseClient = Nothing
		Private _Postgresql As DatabaseWrapper.Postgresql.DatabaseClient = Nothing
		Private _Sqlite As DatabaseWrapper.Sqlite.DatabaseClient = Nothing
		Private _SqlServer As DatabaseWrapper.SqlServer.DatabaseClient = Nothing

		Private _Random As New Random()

		#End Region

		#Region "Constructors-and-Factories"

		''' <summary>
		''' Create an instance of the database client.
		''' </summary>
		''' <param name="settings">Database settings.</param>
		Public Sub New(ByVal settings As DatabaseSettings)
			' TODO TASK: Throw expressions are not converted by :
			'ORIGINAL LINE: _Settings = settings ?? throw new ArgumentNullException(nameof(settings));
			'	_Settings = If(settings, throw New ArgumentNullException(NameOf(settings)))

			If settings Is Nothing Then
				Throw New ArgumentNullException(NameOf(settings))
			Else
				_Settings = settings
			End If

			If _Settings.Type = DbTypes.Sqlite AndAlso String.IsNullOrEmpty(_Settings.Filename) Then
				Throw New ArgumentException("Filename must be populated in database settings of type 'Sqlite'.")
			End If

			If _Settings.Type <> DbTypes.SqlServer AndAlso Not String.IsNullOrEmpty(_Settings.Instance) Then
				Throw New ArgumentException("Instance can only be used in database settings of type 'SqlServer'.")
			End If

			_Header = "[DatabaseWrapper." & _Settings.Type.ToString() & "] "

			Select Case _Settings.Type
				Case DbTypes.Sqlite
					_Sqlite = New Sqlite.DatabaseClient(_Settings)
				Case DbTypes.Mysql
					_Mysql = New Mysql.DatabaseClient(_Settings)
				Case DbTypes.Postgresql
					_Postgresql = New Postgresql.DatabaseClient(_Settings)
				Case DbTypes.SqlServer
					_SqlServer = New SqlServer.DatabaseClient(_Settings)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Create an instance of the database client for a Sqlite database file.
		''' </summary>
		''' <param name="filename">Sqlite database.</param>
		Public Sub New(ByVal filename As String)
			If String.IsNullOrEmpty(filename) Then
				Throw New ArgumentNullException(NameOf(filename))
			End If
			_Settings = New DatabaseSettings(filename)
			_Header = "[DatabaseWrapper." & _Settings.Type.ToString() & "] "
			_Sqlite = New Sqlite.DatabaseClient(filename)
		End Sub

		''' <summary>
		''' Create an instance of the database client.
		''' </summary>
		''' <param name="dbType">The type of database.</param>
		''' <param name="serverIp">The IP address or hostname of the database server.</param>
		''' <param name="serverPort">The TCP port of the database server.</param>
		''' <param name="username">The username to use when authenticating with the database server.</param>
		''' <param name="password">The password to use when authenticating with the database server.</param>
		''' <param name="instance">The instance on the database server (for use with Microsoft SQL Server).</param>
		''' <param name="database">The name of the database with which to connect.</param>
		Public Sub New(ByVal dbType As DbTypes, ByVal serverIp As String, ByVal serverPort As Integer, ByVal username As String, ByVal password As String, ByVal instance As String, ByVal database As String)
			If dbType = DbTypes.Sqlite Then
				Throw New ArgumentException("Use the filename constructor for Sqlite databases.")
			End If
			If String.IsNullOrEmpty(serverIp) Then
				Throw New ArgumentNullException(NameOf(serverIp))
			End If
			If serverPort < 0 Then
				Throw New ArgumentOutOfRangeException(NameOf(serverPort))
			End If
			If String.IsNullOrEmpty(database) Then
				Throw New ArgumentNullException(NameOf(database))
			End If

			If dbType = DbTypes.SqlServer Then
				_Settings = New DatabaseSettings(serverIp, serverPort, username, password, instance, database)
			Else
				If Not String.IsNullOrEmpty(instance) Then
					Throw New ArgumentException("Instance can only be used in database settings of type 'SqlServer'.")
				End If

				_Settings = New DatabaseSettings(dbType, serverIp, serverPort, username, password, database)
			End If

			_Header = "[DatabaseWrapper." & _Settings.Type.ToString() & "] "

			Select Case _Settings.Type
				Case DbTypes.Sqlite
					Throw New ArgumentException("Unable to use this constructor with 'DbTypes.Sqlite'.")
				Case DbTypes.Mysql
					_Mysql = New Mysql.DatabaseClient(_Settings)
				Case DbTypes.Postgresql
					_Postgresql = New Postgresql.DatabaseClient(_Settings)
				Case DbTypes.SqlServer
					_SqlServer = New SqlServer.DatabaseClient(_Settings)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Create an instance of the database client.
		''' </summary>
		''' <param name="dbType">The type of database.</param>
		''' <param name="serverIp">The IP address or hostname of the database server.</param>
		''' <param name="serverPort">The TCP port of the database server.</param>
		''' <param name="username">The username to use when authenticating with the database server.</param>
		''' <param name="password">The password to use when authenticating with the database server.</param> 
		''' <param name="database">The name of the database with which to connect.</param>
		Public Sub New(ByVal dbType As DbTypes, ByVal serverIp As String, ByVal serverPort As Integer, ByVal username As String, ByVal password As String, ByVal database As String)
			If String.IsNullOrEmpty(serverIp) Then
				Throw New ArgumentNullException(NameOf(serverIp))
			End If
			If serverPort < 0 Then
				Throw New ArgumentOutOfRangeException(NameOf(serverPort))
			End If
			If String.IsNullOrEmpty(database) Then
				Throw New ArgumentNullException(NameOf(database))
			End If

			_Settings = New DatabaseSettings(dbType, serverIp, serverPort, username, password, database)
			_Header = "[DatabaseWrapper." & _Settings.Type.ToString() & "] "

			Select Case _Settings.Type
				Case DbTypes.Sqlite
					Throw New ArgumentException("Unable to use this constructor with 'DbTypes.Sqlite'.")
				Case DbTypes.Mysql
					_Mysql = New Mysql.DatabaseClient(_Settings)
				Case DbTypes.Postgresql
					_Postgresql = New Postgresql.DatabaseClient(_Settings)
				Case DbTypes.SqlServer
					_SqlServer = New SqlServer.DatabaseClient(_Settings)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
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
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.ListTables()
				Case DbTypes.Postgresql
					Return _Postgresql.ListTables()
				Case DbTypes.Sqlite
					Return _Sqlite.ListTables()
				Case DbTypes.SqlServer
					Return _SqlServer.ListTables()
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Check if a table exists in the database.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <returns>True if exists.</returns>
		Public Function TableExists(ByVal tableName As String) As Boolean
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.TableExists(tableName)
				Case DbTypes.Postgresql
					Return _Postgresql.TableExists(tableName)
				Case DbTypes.Sqlite
					Return _Sqlite.TableExists(tableName)
				Case DbTypes.SqlServer
					Return _SqlServer.TableExists(tableName)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Show the columns and column metadata from a specific table.
		''' </summary>
		''' <param name="tableName">The table to view.</param>
		''' <returns>A list of column objects.</returns>
		Public Function DescribeTable(ByVal tableName As String) As List(Of Column)
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.DescribeTable(tableName)
				Case DbTypes.Postgresql
					Return _Postgresql.DescribeTable(tableName)
				Case DbTypes.Sqlite
					Return _Sqlite.DescribeTable(tableName)
				Case DbTypes.SqlServer
					Return _SqlServer.DescribeTable(tableName)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Describe each of the tables in the database.
		''' </summary>
		''' <returns>Dictionary.  Key is table name, value is List of Column objects.</returns>
		Public Function DescribeDatabase() As Dictionary(Of String, List(Of Column))
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.DescribeDatabase()
				Case DbTypes.Postgresql
					Return _Postgresql.DescribeDatabase()
				Case DbTypes.Sqlite
					Return _Sqlite.DescribeDatabase()
				Case DbTypes.SqlServer
					Return _SqlServer.DescribeDatabase()
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Create a table with a specified name.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="columns">Columns.</param>
		Public Sub CreateTable(ByVal tableName As String, ByVal columns As List(Of Column))
			Select Case _Settings.Type
				Case DbTypes.Mysql
					_Mysql.CreateTable(tableName, columns)
					Return
				Case DbTypes.Postgresql
					_Postgresql.CreateTable(tableName, columns)
					Return
				Case DbTypes.Sqlite
					_Sqlite.CreateTable(tableName, columns)
					Return
				Case DbTypes.SqlServer
					_SqlServer.CreateTable(tableName, columns)
					Return
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Drop the specified table.  
		''' </summary>
		''' <param name="tableName">The table to drop.</param>
		Public Sub DropTable(ByVal tableName As String)
			Select Case _Settings.Type
				Case DbTypes.Mysql
					_Mysql.DropTable(tableName)
					Return
				Case DbTypes.Postgresql
					_Postgresql.DropTable(tableName)
					Return
				Case DbTypes.Sqlite
					_Sqlite.DropTable(tableName)
					Return
				Case DbTypes.SqlServer
					_SqlServer.DropTable(tableName)
					Return
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Retrieve the name of the primary key column from a specific table.
		''' </summary>
		''' <param name="tableName">The table of which you want the primary key.</param>
		''' <returns>A string containing the column name.</returns>
		Public Function GetPrimaryKeyColumn(ByVal tableName As String) As String
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.GetPrimaryKeyColumn(tableName)
				Case DbTypes.Postgresql
					Return _Postgresql.GetPrimaryKeyColumn(tableName)
				Case DbTypes.Sqlite
					Return _Sqlite.GetPrimaryKeyColumn(tableName)
				Case DbTypes.SqlServer
					Return _SqlServer.GetPrimaryKeyColumn(tableName)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Retrieve a list of the names of columns from within a specific table.
		''' </summary>
		''' <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
		''' <returns>A list of strings containing the column names.</returns>
		Public Function GetColumnNames(ByVal tableName As String) As List(Of String)
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.GetColumnNames(tableName)
				Case DbTypes.Postgresql
					Return _Postgresql.GetColumnNames(tableName)
				Case DbTypes.Sqlite
					Return _Sqlite.GetColumnNames(tableName)
				Case DbTypes.SqlServer
					Return _SqlServer.GetColumnNames(tableName)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
		''' </summary>
		''' <param name="tableName">The table from which you wish to SELECT.</param>
		''' <param name="columnName">The column containing key or unique fields where a match is desired.</param>
		''' <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
		''' <returns>A DataTable containing at most one row.</returns>
		Public Function GetUniqueObjectById(ByVal tableName As String, ByVal columnName As String, ByVal value As Object) As DataTable
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.GetUniqueObjectById(tableName, columnName, value)
				Case DbTypes.Postgresql
					Return _Postgresql.GetUniqueObjectById(tableName, columnName, value)
				Case DbTypes.Sqlite
					Return _Sqlite.GetUniqueObjectById(tableName, columnName, value)
				Case DbTypes.SqlServer
					Return _SqlServer.GetUniqueObjectById(tableName, columnName, value)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
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
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Select(tableName, indexStart, maxResults, returnFields, filter, Nothing)
				Case DbTypes.Postgresql
					Return _Postgresql.Select(tableName, indexStart, maxResults, returnFields, filter, Nothing)
				Case DbTypes.Sqlite
					Return _Sqlite.Select(tableName, indexStart, maxResults, returnFields, filter, Nothing)
				Case DbTypes.SqlServer
					Return _SqlServer.Select(tableName, indexStart, maxResults, returnFields, filter, Nothing)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
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
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder)
				Case DbTypes.Postgresql
					Return _Postgresql.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder)
				Case DbTypes.Sqlite
					Return _Sqlite.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder)
				Case DbTypes.SqlServer
					Return _SqlServer.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Execute an INSERT query.
		''' </summary>
		''' <param name="tableName">The table in which you wish to INSERT.</param>
		''' <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
		''' <returns>A DataTable containing the results.</returns>
		Public Function Insert(ByVal tableName As String, ByVal keyValuePairs As Dictionary(Of String, Object)) As DataTable
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Insert(tableName, keyValuePairs)
				Case DbTypes.Postgresql
					Return _Postgresql.Insert(tableName, keyValuePairs)
				Case DbTypes.Sqlite
					Return _Sqlite.Insert(tableName, keyValuePairs)
				Case DbTypes.SqlServer
					Return _SqlServer.Insert(tableName, keyValuePairs)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Execute an INSERT query with multiple values within a transaction.
		''' </summary>
		''' <param name="tableName">The table in which you wish to INSERT.</param>
		''' <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
		Public Sub InsertMultiple(ByVal tableName As String, ByVal keyValuePairList As List(Of Dictionary(Of String, Object)))
			Select Case _Settings.Type
				Case DbTypes.Mysql
					_Mysql.InsertMultiple(tableName, keyValuePairList)
					Return
				Case DbTypes.Postgresql
					_Postgresql.InsertMultiple(tableName, keyValuePairList)
					Return
				Case DbTypes.Sqlite
					_Sqlite.InsertMultiple(tableName, keyValuePairList)
					Return
				Case DbTypes.SqlServer
					_SqlServer.InsertMultiple(tableName, keyValuePairList)
					Return
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Execute an UPDATE query.
		''' For Microsoft SQL Server and PostgreSQL, the updated rows are returned.
		''' For MySQL and Sqlite, nothing is returned.
		''' </summary>
		''' <param name="tableName">The table in which you wish to UPDATE.</param>
		''' <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
		''' <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
		''' <returns>For Microsoft SQL Server and PostgreSQL, a DataTable containing the results.  For MySQL and Sqlite, null.</returns>
		Public Function Update(ByVal tableName As String, ByVal keyValuePairs As Dictionary(Of String, Object), ByVal filter As Expr) As DataTable
			Select Case _Settings.Type
				Case DbTypes.Mysql
					_Mysql.Update(tableName, keyValuePairs, filter)
					Return Nothing
				Case DbTypes.Postgresql
					Return _Postgresql.Update(tableName, keyValuePairs, filter)
				Case DbTypes.Sqlite
					_Sqlite.Update(tableName, keyValuePairs, filter)
					Return Nothing
				Case DbTypes.SqlServer
					Return _SqlServer.Update(tableName, keyValuePairs, filter)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Execute a DELETE query.
		''' </summary>
		''' <param name="tableName">The table in which you wish to DELETE.</param>
		''' <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
		Public Sub Delete(ByVal tableName As String, ByVal filter As Expr)
			Select Case _Settings.Type
				Case DbTypes.Mysql
					_Mysql.Delete(tableName, filter)
					Return
				Case DbTypes.Postgresql
					_Postgresql.Delete(tableName, filter)
					Return
				Case DbTypes.Sqlite
					_Sqlite.Delete(tableName, filter)
					Return
				Case DbTypes.SqlServer
					_SqlServer.Delete(tableName, filter)
					Return
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Empties a table completely.
		''' </summary>
		''' <param name="tableName">The table you wish to TRUNCATE.</param>
		Public Sub Truncate(ByVal tableName As String)
			Select Case _Settings.Type
				Case DbTypes.Mysql
					_Mysql.Truncate(tableName)
					Return
				Case DbTypes.Postgresql
					_Postgresql.Truncate(tableName)
					Return
				Case DbTypes.Sqlite
					_Sqlite.Truncate(tableName)
					Return
				Case DbTypes.SqlServer
					_SqlServer.Truncate(tableName)
					Return
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Sub

		''' <summary>
		''' Execute a query.
		''' </summary>
		''' <param name="query_Conflict">Database query defined outside of the database client.</param>
		''' <returns>A DataTable containing the results.</returns>
		' NOTE: The parameter query was renamed since Visual Basic will not allow parameters with the same name as their enclosing function or property:
		Public Function Query(ByVal query_Conflict As String) As DataTable
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Query(query_Conflict)
				Case DbTypes.Postgresql
					Return _Postgresql.Query(query_Conflict)
				Case DbTypes.Sqlite
					Return _Sqlite.Query(query_Conflict)
				Case DbTypes.SqlServer
					Return _SqlServer.Query(query_Conflict)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Determine if records exist by filter.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="filter">Expression.</param>
		''' <returns>True if records exist.</returns>
		Public Function Exists(ByVal tableName As String, ByVal filter As Expr) As Boolean
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Exists(tableName, filter)
				Case DbTypes.Postgresql
					Return _Postgresql.Exists(tableName, filter)
				Case DbTypes.Sqlite
					Return _Sqlite.Exists(tableName, filter)
				Case DbTypes.SqlServer
					Return _SqlServer.Exists(tableName, filter)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Determine the number of records that exist by filter.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="filter">Expression.</param>
		''' <returns>The number of records.</returns>
		Public Function Count(ByVal tableName As String, ByVal filter As Expr) As Long
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Count(tableName, filter)
				Case DbTypes.Postgresql
					Return _Postgresql.Count(tableName, filter)
				Case DbTypes.Sqlite
					Return _Sqlite.Count(tableName, filter)
				Case DbTypes.SqlServer
					Return _SqlServer.Count(tableName, filter)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Determine the sum of a column for records that match the supplied filter.
		''' </summary>
		''' <param name="tableName">The name of the table.</param>
		''' <param name="fieldName">The name of the field.</param>
		''' <param name="filter">Expression.</param>
		''' <returns>The sum of the specified column from the matching rows.</returns>
		Public Function Sum(ByVal tableName As String, ByVal fieldName As String, ByVal filter As Expr) As Decimal
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Sum(tableName, fieldName, filter)
				Case DbTypes.Postgresql
					Return _Postgresql.Sum(tableName, fieldName, filter)
				Case DbTypes.Sqlite
					Return _Sqlite.Sum(tableName, fieldName, filter)
				Case DbTypes.SqlServer
					Return _SqlServer.Sum(tableName, fieldName, filter)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Create a string timestamp from the given DateTime for the database of the instance type.
		''' </summary>
		''' <param name="ts">DateTime.</param>
		''' <returns>A string with timestamp formatted for the database of the instance type.</returns>
		Public Function Timestamp(ByVal ts As DateTime) As String
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.Timestamp(ts)
				Case DbTypes.Postgresql
					Return _Postgresql.Timestamp(ts)
				Case DbTypes.Sqlite
					Return _Sqlite.Timestamp(ts)
				Case DbTypes.SqlServer
					Return _SqlServer.Timestamp(ts)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Create a string timestamp with offset from the given DateTimeOffset for the database of the instance type.
		''' </summary>
		''' <param name="ts">DateTimeOffset.</param>
		''' <returns>A string with timestamp and offset formatted for the database of the instance type.</returns>
		Public Function TimestampOffset(ByVal ts As DateTimeOffset) As String
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.TimestampOffset(ts)
				Case DbTypes.Postgresql
					Return _Postgresql.TimestampOffset(ts)
				Case DbTypes.Sqlite
					Return _Sqlite.TimestampOffset(ts)
				Case DbTypes.SqlServer
					Return _SqlServer.TimestampOffset(ts)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
		End Function

		''' <summary>
		''' Sanitize an input string.
		''' </summary>
		''' <param name="s">The value to sanitize.</param>
		''' <returns>A sanitized string.</returns>
		Public Function SanitizeString(ByVal s As String) As String
			Select Case _Settings.Type
				Case DbTypes.Mysql
					Return _Mysql.SanitizeString(s)
				Case DbTypes.Postgresql
					Return _Postgresql.SanitizeString(s)
				Case DbTypes.Sqlite
					Return _Sqlite.SanitizeString(s)
				Case DbTypes.SqlServer
					Return _SqlServer.SanitizeString(s)
				Case Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
			End Select
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
				If _Mysql IsNot Nothing Then
					_Mysql.Dispose()
				ElseIf _Postgresql IsNot Nothing Then
					_Postgresql.Dispose()
				ElseIf _Sqlite IsNot Nothing Then
					_Sqlite.Dispose()
				ElseIf _SqlServer IsNot Nothing Then
					_SqlServer.Dispose()
				Else
					Throw New ArgumentException("Unknown database type '" & _Settings.Type.ToString() & "'.")
				End If
			End If

			_Disposed = True
		End Sub

		#End Region

		#Region "Public-Static-Methods"

		''' <summary>
		''' Convert a DateTime to a string formatted for the specified database type.
		''' </summary>
		''' <param name="dbType">The type of database.</param>
		''' <param name="ts">The timestamp.</param>
		''' <returns>A string formatted for use with the specified database.</returns>
		Public Shared Function DbTimestamp(ByVal dbType As DbTypes, ByVal ts As DateTime) As String
			Select Case dbType
				Case DbTypes.Mysql
					Return Mysql.DatabaseClient.DbTimestamp(ts)
				Case DbTypes.Postgresql
					Return Postgresql.DatabaseClient.DbTimestamp(ts)
				Case DbTypes.Sqlite
					Return Sqlite.DatabaseClient.DbTimestamp(ts)
				Case DbTypes.SqlServer
					Return SqlServer.DatabaseClient.DbTimestamp(ts)
				Case Else
					Return Nothing
			End Select
		End Function

		''' <summary>
		''' Convert a DateTimeOffset to a string formatted for the specified database type.
		''' </summary>
		''' <param name="dbType">The type of database.</param>
		''' <param name="ts">The timestamp with offset.</param>
		''' <returns>A string formatted for use with the specified database.</returns>
		Public Shared Function DbTimestampOffset(ByVal dbType As DbTypes, ByVal ts As DateTimeOffset) As String
			Select Case dbType
				Case DbTypes.Mysql
					Return Mysql.DatabaseClient.DbTimestampOffset(ts)
				Case DbTypes.Postgresql
					Return Postgresql.DatabaseClient.DbTimestampOffset(ts)
				Case DbTypes.Sqlite
					Return Sqlite.DatabaseClient.DbTimestampOffset(ts)
				Case DbTypes.SqlServer
					Return SqlServer.DatabaseClient.DbTimestampOffset(ts)
				Case Else
					Return Nothing
			End Select
		End Function

		#End Region
	End Class
End Namespace
