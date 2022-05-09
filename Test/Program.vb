﻿Imports System
Imports System.Collections.Generic
Imports System.Data
Imports System.Diagnostics
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports DatabaseWrapper
Imports DatabaseWrapper.Core
Imports ExpressionTree

Namespace Test
	Friend Class Program
		Private Shared _Random As New Random(DateTime.Now.Millisecond)
		Private Shared _Settings As DatabaseSettings
		Private Shared _Database As DatabaseClient

		' To use dbo.person, change _Table to either 'dbo.person' or just '" + _Table + "'
		' To use with a specific schema, use 'schema.table', i.e. 'foo.person'
		Private Shared _Table As String = "person"

		Shared Sub Main(ByVal args() As String)
			Try
'				#Region "Select-Database-Type"

'                
'                 * 
'                 * 
'                 * Create the database 'test' before proceeding if using mssql, mysql, or pgsql
'                 * 
'                 * 
'                 

				Console.Write("DB type [sqlserver|mysql|postgresql|sqlite]: ")
				Dim dbType As String = Console.ReadLine()
				If String.IsNullOrEmpty(dbType) Then
					Return
				End If
				dbType = dbType.ToLower()

				If dbType.Equals("sqlserver") OrElse dbType.Equals("mysql") OrElse dbType.Equals("postgresql") Then
					Console.Write("User: ")
					Dim user As String = Console.ReadLine()

					Console.Write("Password: ")
					Dim pass As String = Console.ReadLine()

					Select Case dbType
						Case "sqlserver"
							_Settings = New DatabaseSettings("localhost", 1433, user, pass, Nothing, "test")
							_Database = New DatabaseClient(_Settings)
						Case "mysql"
							_Settings = New DatabaseSettings(DbTypes.Mysql, "localhost", 3306, user, pass, "test")
							_Database = New DatabaseClient(_Settings)
						Case "postgresql"
							_Settings = New DatabaseSettings(DbTypes.Postgresql, "localhost", 5432, user, pass, "test")
							_Database = New DatabaseClient(_Settings)
						Case Else
							Return
					End Select
				ElseIf dbType.Equals("sqlite") Then
					Console.Write("Filename: ")
					Dim filename As String = Console.ReadLine()
					If String.IsNullOrEmpty(filename) Then
						Return
					End If

					_Settings = New DatabaseSettings(filename)
					_Database = New DatabaseClient(_Settings)
				Else
					Console.WriteLine("Invalid database type.")
					Return
				End If

				_Database.Logger = AddressOf Logger
				_Database.LogQueries = True
				_Database.LogResults = True

'				#End Region

'				#Region "Drop-Table"

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Dropping table '" & _Table & "'...")
				_Database.DropTable(_Table)
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

'				#End Region

'				#Region "Create-Table"

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Creating table '" & _Table & "'...")
				Dim columns As New List(Of Column)()
				columns.Add(New Column("id", True, DataType.Int, 11, Nothing, False))
				columns.Add(New Column("firstname", False, DataType.Nvarchar, 30, Nothing, False))
				columns.Add(New Column("lastname", False, DataType.Nvarchar, 30, Nothing, False))
				columns.Add(New Column("age", False, DataType.Int, 11, Nothing, True))
				columns.Add(New Column("value", False, DataType.Long, 12, Nothing, True))
				columns.Add(New Column("birthday", False, DataType.DateTime, Nothing, Nothing, True))
				columns.Add(New Column("hourly", False, DataType.Decimal, 18, 2, True))
				columns.Add(New Column("localtime", False, DataType.DateTimeOffset, Nothing, Nothing, True))

				_Database.CreateTable(_Table, columns)
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

'				#End Region

'				#Region "Check-Existence-and-Describe"

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Table '" & _Table & "' exists: " & _Database.TableExists(_Table))
				Console.WriteLine("Table '" & _Table & "' configuration:")
				columns = _Database.DescribeTable(_Table)
				For Each col As Column In columns
					Console.WriteLine(col.ToString())
				Next col
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

'				#End Region

'				#Region "Load-Update-Retrieve-Delete"

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Loading rows...")
				LoadRows()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				Console.WriteLine("Checking existence...")
				ExistsRows()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				Console.WriteLine("Counting age...")
				CountAge()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				Console.WriteLine("Summing age...")
				SumAge()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Updating rows...")
				UpdateRows()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Retrieving rows...")
				RetrieveRows()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Retrieving rows with special character...")
				RetrieveRowsWithSpecialCharacter()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Retrieving rows by index...")
				RetrieveRowsByIndex()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Retrieving rows by between...")
				RetrieveRowsByBetween()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Retrieving sorted rows...")
				RetrieveRowsSorted()
				Console.WriteLine("Press ENTER to continue...")
				Console.ReadLine()

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Deleting rows...")
				DeleteRows()
				Console.WriteLine("Press ENTER to continue")
				Console.ReadLine()

'				#End Region

'				#Region "Cause-Exception"

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Testing exception...")

				Try
					_Database.Query("SELECT * FROM person(((")
				Catch e As Exception
					Console.WriteLine("Caught exception: " & e.Message)
					Console.WriteLine("Query: " & e.Data.ToString().Chars("Query"))
				End Try

'				#End Region

'				#Region "Drop-Table"

				For i As Integer = 0 To 23
					Console.WriteLine("")
				Next i
				Console.WriteLine("Dropping table...")
				_Database.DropTable(_Table)
				Console.ReadLine()

'				#End Region
			Catch e As Exception
				ExceptionConsole("Main", "Outer exception", e)
			End Try
		End Sub

		Private Shared Sub LoadRows()
			For i As Integer = 0 To 49
				Dim d As New Dictionary(Of String, Object)()
				d.Add("firstname", "first" & i)
				d.Add("lastname", "last" & i)
				d.Add("age", i)
				d.Add("value", i * 1000)
				d.Add("birthday", DateTime.Now)
				d.Add("hourly", 123.456)
				d.Add("localtime", New DateTimeOffset(2021, 4, 14, 01, 02, 03, New TimeSpan(7, 0, 0)))

				_Database.Insert(_Table, d)
			Next i

			For i As Integer = 0 To 9
				Dim d As New Dictionary(Of String, Object)()
				d.Add("firstname", "firsté" & i)
				d.Add("lastname", "lasté" & i)
				d.Add("age", i)
				d.Add("value", i * 1000)
				d.Add("birthday", DateTime.Now)
				d.Add("hourly", 123.456)
				d.Add("localtime", New DateTimeOffset(2021, 4, 14, 01, 02, 03, New TimeSpan(7, 0, 0)))

				_Database.Insert(_Table, d)
			Next i
		End Sub

		Private Shared Sub ExistsRows()
			Dim e As New Expr("firstname", OperatorEnum.IsNotNull, Nothing)
			Console.WriteLine("Exists: " & _Database.Exists(_Table, e))
		End Sub

		Private Shared Sub CountAge()
			Dim e As New Expr("age", OperatorEnum.GreaterThan, 25)
			Console.WriteLine("Age count: " & _Database.Count(_Table, e))
		End Sub

		Private Shared Sub SumAge()
			Dim e As New Expr("age", OperatorEnum.GreaterThan, 0)
			Console.WriteLine("Age sum: " & _Database.Sum(_Table, "age", e))
		End Sub

		Private Shared Sub UpdateRows()
			For i As Integer = 10 To 19
				Dim d As New Dictionary(Of String, Object)()
				d.Add("firstname", "first" & i & "-updated")
				d.Add("lastname", "last" & i & "-updated")
				d.Add("age", i)

				Dim e As New Expr("id", OperatorEnum.Equals, i)
				_Database.Update(_Table, d, e)
			Next i
		End Sub

		Private Shared Sub RetrieveRows()
			Dim returnFields As New List(Of String) From {"firstname", "lastname", "age"}

			For i As Integer = 30 To 39
				Dim e As New Expr With {
					.Left = New Expr("id", OperatorEnum.LessThan, i),
					.Operator = OperatorEnum.And,
					.Right = New Expr("age", OperatorEnum.LessThan, i)
				}

				' 
				' Yes, personId and age should be the same, however, the example
				' is here to show how to build a nested expression
				'

				Dim resultOrder(0) As ResultOrder
				resultOrder(0) = New ResultOrder("id", OrderDirection.Ascending)

				_Database.Select(_Table, 0, 3, returnFields, e, resultOrder)
			Next i
		End Sub

		Private Shared Sub RetrieveRowsWithSpecialCharacter()
			Dim returnFields As New List(Of String) From {"firstname", "lastname", "age"}

			Dim e As New Expr("lastname", OperatorEnum.StartsWith, "lasté")

			Dim result As DataTable = _Database.Select(_Table, 0, 5, returnFields, e)
			If result IsNot Nothing AndAlso result.Rows IsNot Nothing AndAlso result.Rows.Count > 0 Then
				For Each row As DataRow In result.Rows
					Console.WriteLine("Person: " & row("firstname") & " " & row("lastname") & " age: " & row("age"))
				Next row
			End If
		End Sub

		Private Shared Sub RetrieveRowsByIndex()
			Dim returnFields As New List(Of String) From {"firstname", "lastname", "age"}

			For i As Integer = 10 To 19
				Dim e As New Expr With {
					.Left = New Expr("id", OperatorEnum.GreaterThan, 1),
					.Operator = OperatorEnum.And,
					.Right = New Expr("age", OperatorEnum.LessThan, 50)
				}

				' 
				' Yes, personId and age should be the same, however, the example
				' is here to show how to build a nested expression
				'

				Dim resultOrder(0) As ResultOrder
				resultOrder(0) = New ResultOrder("id", OrderDirection.Ascending)

				_Database.Select(_Table, (i - 10), 5, returnFields, e, resultOrder)
			Next i
		End Sub

		Private Shared Sub RetrieveRowsByBetween()
			Dim returnFields As New List(Of String) From {"firstname", "lastname", "age"}
			Dim e As Expr = Expr.Between("id", New List(Of Object) From {10, 20})
			Console.WriteLine("Expression: " & e.ToString())
			_Database.Select(_Table, Nothing, Nothing, returnFields, e)
		End Sub

		Private Shared Sub RetrieveRowsSorted()
			Dim returnFields As New List(Of String) From {"firstname", "lastname", "age"}
			Dim e As Expr = Expr.Between("id", New List(Of Object) From {10, 20})
			Console.WriteLine("Expression: " & e.ToString())
			Dim resultOrder(0) As ResultOrder
			resultOrder(0) = New ResultOrder("firstname", OrderDirection.Ascending)
			_Database.Select(_Table, Nothing, Nothing, returnFields, e, resultOrder)
		End Sub

		Private Shared Sub DeleteRows()
			For i As Integer = 20 To 29
				Dim e As New Expr("id", OperatorEnum.Equals, i)
				_Database.Delete(_Table, e)
			Next i
		End Sub

		Private Shared Function StackToString() As String
			Dim ret As String = ""

			Dim t As New StackTrace()
			For i As Integer = 0 To t.FrameCount - 1
				If i = 0 Then
					ret &= t.GetFrame(i).GetMethod().Name
				Else
					ret &= " <= " & t.GetFrame(i).GetMethod().Name
				End If
			Next i

			Return ret
		End Function

		Private Shared Sub ExceptionConsole(ByVal method As String, ByVal text As String, ByVal e As Exception)
			Dim st = New StackTrace(e, True)
			Dim frame = st.GetFrame(0)
			Dim line As Integer = frame.GetFileLineNumber()
			Dim filename As String = frame.GetFileName()

			Console.WriteLine("---")
			Console.WriteLine("An exception was encountered which triggered this message.")
			Console.WriteLine("  Method: " & method)
			Console.WriteLine("  Text: " & text)
			Console.WriteLine("  Type: " & e.GetType().ToString())
			Console.WriteLine("  Data: " & e.Data.ToString())
			Console.WriteLine("  Inner: " & e.InnerException.ToString)
			Console.WriteLine("  Message: " & e.Message)
			Console.WriteLine("  Source: " & e.Source)
			Console.WriteLine("  StackTrace: " & e.StackTrace)
			Console.WriteLine("  Stack: " & StackToString())
			Console.WriteLine("  Line: " & line)
			Console.WriteLine("  File: " & filename)
			Console.WriteLine("  ToString: " & e.ToString())
			Console.WriteLine("---")

			Return
		End Sub

		Private Shared Sub Logger(ByVal msg As String)
			Console.WriteLine(msg)
		End Sub
	End Class
End Namespace