Imports System
Imports System.Collections.Generic
Imports System.Text

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Database settings.
	''' </summary>
	Public Class DatabaseSettings
		#Region "Public-Members"

		''' <summary>
		''' Filename, if using Sqlite.
		''' </summary>
		Public Property Filename() As String = Nothing

		''' <summary>
		''' The type of database.
		''' </summary>
		Public Property Type() As DbTypes = DbTypes.Sqlite

		''' <summary>
		''' The hostname of the database server.
		''' </summary>
		Public Property Hostname() As String = Nothing

		''' <summary>
		''' The TCP port number on which the server is listening.
		''' </summary>
		Public Property Port() As Integer = 0

		''' <summary>
		''' The username to use when accessing the database.
		''' </summary>
		Public Property Username() As String = Nothing

		''' <summary>
		''' The password to use when accessing the database.
		''' </summary>
		Public Property Password() As String = Nothing

		''' <summary>
		''' For SQL Server Express, the instance name.
		''' </summary>
		Public Property Instance() As String = Nothing

		''' <summary>
		''' The name of the database.
		''' </summary>
		Public Property DatabaseName() As String = Nothing

		#End Region

		#Region "Private-Members"

		#End Region

		#Region "Constructors-and-Factories"

		''' <summary>
		''' Instantiate the object.
		''' </summary>
		Public Sub New()

		End Sub

		''' <summary>
		''' Instantiate the object using Sqlite.
		''' </summary>
		''' <param name="filename">The Sqlite database filename.</param>
		Public Sub New(ByVal filename As String)
			If String.IsNullOrEmpty(filename) Then
				Throw New ArgumentNullException(NameOf(filename))
			End If
			Type = DbTypes.Sqlite
			Me.Filename = filename
		End Sub

		''' <summary>
		''' Instantiate the object using SQL Server, MySQL, or PostgreSQL.
		''' </summary>
		''' <param name="dbType">The type of database.</param>
		''' <param name="hostname">The hostname of the database server.</param>
		''' <param name="port">The TCP port number on which the server is listening.</param>
		''' <param name="username">The username to use when accessing the database.</param>
		''' <param name="password">The password to use when accessing the database.</param> 
		''' <param name="dbName">The name of the database.</param>
		Public Sub New(ByVal dbType As String, ByVal hostname As String, ByVal port As Integer, ByVal username As String, ByVal password As String, ByVal dbName As String)
			If String.IsNullOrEmpty(dbType) Then
				Throw New ArgumentNullException(NameOf(dbType))
			End If
			If String.IsNullOrEmpty(hostname) Then
				Throw New ArgumentNullException(NameOf(hostname))
			End If
			If String.IsNullOrEmpty(dbName) Then
				Throw New ArgumentNullException(NameOf(dbName))
			End If

			Type = DirectCast(System.Enum.Parse(GetType(DbTypes), dbType), DbTypes)
			If Type = DbTypes.Sqlite Then
				Throw New ArgumentException("For Sqlite, use the filename constructor.")
			End If

			Me.Hostname = hostname
			Me.Port = port
			Me.Username = username
			Me.Password = password
			Instance = Nothing
			DatabaseName = dbName
		End Sub

		''' <summary>
		''' Instantiate the object using SQL Server, MySQL, or PostgreSQL.
		''' </summary>
		''' <param name="dbType">The type of database.</param>
		''' <param name="hostname">The hostname of the database server.</param>
		''' <param name="port">The TCP port number on which the server is listening.</param>
		''' <param name="username">The username to use when accessing the database.</param>
		''' <param name="password">The password to use when accessing the database.</param> 
		''' <param name="dbName">The name of the database.</param>
		Public Sub New(ByVal dbType As DbTypes, ByVal hostname As String, ByVal port As Integer, ByVal username As String, ByVal password As String, ByVal dbName As String)
			If String.IsNullOrEmpty(hostname) Then
				Throw New ArgumentNullException(NameOf(hostname))
			End If
			If String.IsNullOrEmpty(dbName) Then
				Throw New ArgumentNullException(NameOf(dbName))
			End If

			Type = dbType
			If Type = DbTypes.Sqlite Then
				Throw New ArgumentException("For Sqlite, use the filename constructor for DatabaseSettings.")
			End If

			Me.Hostname = hostname
			Me.Port = port
			Me.Username = username
			Me.Password = password
			Instance = Nothing
			DatabaseName = dbName
		End Sub

		''' <summary>
		''' Instantiate the object for SQL Server Express.
		''' </summary> 
		''' <param name="hostname">The hostname of the database server.</param>
		''' <param name="port">The TCP port number on which the server is listening.</param>
		''' <param name="username">The username to use when accessing the database.</param>
		''' <param name="password">The password to use when accessing the database.</param>
		''' <param name="instance">For SQL Server Express, the instance name.</param>
		''' <param name="dbName">The name of the database.</param>
		Public Sub New(ByVal hostname As String, ByVal port As Integer, ByVal username As String, ByVal password As String, ByVal instance As String, ByVal dbName As String)
			If String.IsNullOrEmpty(hostname) Then
				Throw New ArgumentNullException(NameOf(hostname))
			End If
			If String.IsNullOrEmpty(dbName) Then
				Throw New ArgumentNullException(NameOf(dbName))
			End If

			Type = DbTypes.SqlServer
			Me.Hostname = hostname
			Me.Port = port
			Me.Username = username
			Me.Password = password
			Me.Instance = instance
			DatabaseName = dbName
		End Sub

		#End Region

		#Region "Public-Methods"

		#End Region

		#Region "Private-Methods"

		#End Region
	End Class
End Namespace