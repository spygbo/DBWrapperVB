Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports System.Runtime.Serialization

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Enumeration containing the supported database types.
	''' </summary>
	Public Enum DbTypes
		''' <summary>
		''' Microsoft SQL Server
		''' </summary>
		<EnumMember(Value := "SqlServer")>
		SqlServer
		''' <summary>
		''' MySQL
		''' </summary>
		<EnumMember(Value := "Mysql")>
		Mysql
		''' <summary>
		''' PostgreSQL
		''' </summary>
		<EnumMember(Value := "Postgresql")>
		Postgresql
		''' <summary>
		''' Sqlite
		''' </summary>
		<EnumMember(Value := "Sqlite")>
		Sqlite
	End Enum
End Namespace