Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports Npgsql
Imports DatabaseWrapper.Core
Imports ExpressionTree

Namespace DatabaseWrapper.Postgresql
	Friend Module PostgresqlHelper
		Friend TimestampFormat As String = "MM/dd/yyyy hh:mm:ss.fffffff tt"

		Friend TimestampOffsetFormat As String = "MM/dd/yyyy hh:mm:ss.fffffff zzz"

		Friend Function ConnectionString(ByVal settings As DatabaseSettings) As String
			Dim ret As String = ""

			'
			' http://www.connectionstrings.com/postgresql/
			'
			' PgSQL does not use 'Instance'
			ret &= "Server=" & settings.Hostname & "; "
			If settings.Port > 0 Then
				ret &= "Port=" & settings.Port & "; "
			End If
			ret &= "Database=" & settings.DatabaseName & "; "
			If Not String.IsNullOrEmpty(settings.Username) Then
				ret &= "User ID=" & settings.Username & "; "
			End If
			If Not String.IsNullOrEmpty(settings.Password) Then
				ret &= "Password=" & settings.Password & "; "
			End If

			Return ret
		End Function

		Friend Function LoadTableNamesQuery() As String
			Return "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
		End Function

		Friend Function LoadTableColumnsQuery(ByVal database As String, ByVal table As String) As String
			Return "SELECT " & "  cols.COLUMN_NAME AS COLUMN_NAME, " & "  cols.IS_NULLABLE AS IS_NULLABLE, " & "  cols.DATA_TYPE AS DATA_TYPE, " & "  cols.CHARACTER_MAXIMUM_LENGTH AS CHARACTER_MAXIMUM_LENGTH, " & "  CASE " & "    WHEN cons.COLUMN_NAME IS NULL THEN 'NO' ELSE 'YES' " & "  END AS IS_PRIMARY_KEY " & "FROM test.INFORMATION_SCHEMA.COLUMNS cols " & "LEFT JOIN " & database & ".INFORMATION_SCHEMA.KEY_COLUMN_USAGE cons ON cols.COLUMN_NAME = cons.COLUMN_NAME " & "WHERE cols.TABLE_NAME = '" & ExtractTableName(table) & "';"
		End Function

		Friend Function SanitizeString(ByVal val As String) As String
			Dim tag As String = "$" & EscapeString(val, 2) & "$"
			Return tag & val & tag
		End Function

		Friend Function EscapeString(ByVal val As String, ByVal numChar As Integer) As String
			Dim ret As String = ""
			Dim random As New Random()
			If numChar < 1 Then
				Return ret
			End If

			Do
				ret = ""
				random = New Random()

				Dim valid As Integer = 0
				Dim num As Integer = 0

				For i As Integer = 0 To numChar - 1
					num = 0
					valid = 0
					Do While valid = 0
						num = random.Next(126)
						If ((num > 64) AndAlso (num < 91)) OrElse ((num > 96) AndAlso (num < 123)) Then
							valid = 1
						End If
					Loop
					ret &= ChrW(num)
				Next i

				If Not val.Contains("$" & ret & "$") Then
					Exit Do
				End If
			Loop

			Return ret
		End Function

		Friend Function SanitizeFieldname(ByVal val As String) As String
			Dim ret As String = ""

			'
			' null, below ASCII range, above ASCII range
			'
			For i As Integer = 0 To val.Length - 1
				If (AscW(val.Chars(i)) = 10) OrElse (AscW(val.Chars(i)) = 13) Then ' and line feed
					ret &= val.Chars(i)
				ElseIf AscW(val.Chars(i)) < 32 Then
					Continue For
				Else
					ret &= val.Chars(i)
				End If
			Next i

			'
			' double dash
			'
			Dim doubleDash As Integer = 0
			Do
				doubleDash = ret.IndexOf("--")
				If doubleDash < 0 Then
					Exit Do
				Else
					ret = ret.Remove(doubleDash, 2)
				End If
			Loop

			'
			' open comment
			' 
			Dim openComment As Integer = 0
			Do
				openComment = ret.IndexOf("/*")
				If openComment < 0 Then
					Exit Do
				Else
					ret = ret.Remove(openComment, 2)
				End If
			Loop

			'
			' close comment
			'
			Dim closeComment As Integer = 0
			Do
				closeComment = ret.IndexOf("*/")
				If closeComment < 0 Then
					Exit Do
				Else
					ret = ret.Remove(closeComment, 2)
				End If
			Loop

			'
			' in-string replacement
			'
			ret = ret.Replace("'", "''")
			Return ret
		End Function

		Friend Function ColumnToCreateString(ByVal col As Column) As String
			Dim ret As String = """" & SanitizeFieldname(col.Name) & """ "

			If col.PrimaryKey Then
				ret &= "SERIAL PRIMARY KEY "
				Return ret
			End If

			Select Case col.Type
				Case DataType.Varchar, DataType.Nvarchar
					ret &= "character varying(" & col.MaxLength & ") "
				Case DataType.Int
					ret &= "integer "
				Case DataType.Long
					ret &= "bigint "
				Case DataType.Decimal
					ret &= "numeric(" & col.MaxLength & "," & col.Precision & ") "
				Case DataType.Double
					ret &= "float(" & col.MaxLength & ") "
				Case DataType.DateTime
					ret &= "timestamp without time zone "
				Case DataType.DateTimeOffset
					ret &= "timestamp with time zone "
				Case DataType.Blob
					ret &= "bytea "
				Case Else
					Throw New ArgumentException("Unknown DataType: " & col.Type.ToString())
			End Select

			If col.Nullable Then
				ret &= "NULL "
			Else
				ret &= "NOT NULL "
			End If

			Return ret
		End Function

		Friend Function GetPrimaryKeyColumn(ByVal columns As List(Of Column)) As Column
			Dim c As Column = columns.FirstOrDefault(Function(d) d.PrimaryKey)
			If c Is Nothing Then
				Return Nothing
			End If
			Return c
		End Function

		Friend Function CreateTableQuery(ByVal tableName As String, ByVal columns As List(Of Column)) As String
			Dim query As String = "CREATE TABLE " & PreparedTableName(tableName) & " " & "("

			Dim added As Integer = 0
			For Each curr As Column In columns
				If added > 0 Then
					query &= ", "
				End If
				query &= ColumnToCreateString(curr)
				added += 1
			Next curr

			query &= ") " & "WITH " & "(" & "  OIDS = FALSE" & ")"

			Return query
		End Function

		Friend Function DropTableQuery(ByVal tableName As String) As String
			Dim query As String = "DROP TABLE IF EXISTS " & PreparedTableName(tableName) & " "
			Return query
		End Function

		Friend Function SelectQuery(ByVal tableName As String, ByVal indexStart? As Integer, ByVal maxResults? As Integer, ByVal returnFields As List(Of String), ByVal filter As Expr, ByVal resultOrder() As ResultOrder) As String
			Dim query As String = ""
			Dim whereClause As String = ""

			' SELECT 
			query &= "SELECT "

			' fields 
			If returnFields Is Nothing OrElse returnFields.Count < 1 Then
				query &= "* "
			Else
				Dim fieldsAdded As Integer = 0
				For Each curr As String In returnFields
					If fieldsAdded = 0 Then
						query &= """" & SanitizeFieldname(curr) & """"
						fieldsAdded += 1
					Else
						query &= ",""" & SanitizeFieldname(curr) & """"
						fieldsAdded += 1
					End If
				Next curr
			End If
			query &= " "

			' table 
			query &= "FROM " & PreparedTableName(tableName) & " "

			' expressions 
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			' order clause 
			query &= BuildOrderByClause(resultOrder)

			' limit 
			' WARNING: Comparisons involving nullable type instances require Option Strict Off:
			If maxResults > 0 Then
				' WARNING: Comparisons involving nullable type instances require Option Strict Off:
				If indexStart IsNot Nothing AndAlso indexStart >= 0 Then
					query &= "OFFSET " & indexStart & " LIMIT " & maxResults
				Else
					query &= "LIMIT " & maxResults
				End If
			End If

			Return query
		End Function

		Friend Function PreparedOrderByClause(ByVal val As String) As String
			Dim ret As String = ""

			'
			' null, below ASCII range, above ASCII range
			'
			For i As Integer = 0 To val.Length - 1
				If (AscW(val.Chars(i)) = 10) OrElse (AscW(val.Chars(i)) = 13) Then ' and line feed
					ret &= val.Chars(i)
				ElseIf AscW(val.Chars(i)) < 32 Then
					Continue For
				Else
					ret &= val.Chars(i)
				End If
			Next i

			'
			' double dash
			'
			Dim doubleDash As Integer = 0
			Do
				doubleDash = ret.IndexOf("--")
				If doubleDash < 0 Then
					Exit Do
				Else
					ret = ret.Remove(doubleDash, 2)
				End If
			Loop

			'
			' open comment
			' 
			Dim openComment As Integer = 0
			Do
				openComment = ret.IndexOf("/*")
				If openComment < 0 Then
					Exit Do
				Else
					ret = ret.Remove(openComment, 2)
				End If
			Loop

			'
			' close comment
			'
			Dim closeComment As Integer = 0
			Do
				closeComment = ret.IndexOf("*/")
				If closeComment < 0 Then
					Exit Do
				Else
					ret = ret.Remove(closeComment, 2)
				End If
			Loop

			'
			' in-string replacement
			'
			ret = ret.Replace("'", "''")
			Return ret
		End Function

		Friend Function InsertQuery(ByVal tableName As String, ByVal keys As String, ByVal values As String) As String
			'Dim ret As String = "INSERT INTO " & PreparedTableName(tableName) & " " & "(" & keys & ") " & "VALUES " & "(" & values & ") " & "RETURNING *;"
			Dim ret As String = "INSERT INTO " & PreparedTableName(tableName) & " " & "(" & Sanitise(keys) & ") " & "VALUES " & "(" & keys & ") " & "RETURNING *;"
			Return ret
		End Function
		Public Function Sanitise(ByRef sval As String) As String
			Return sval.Replace("@", "")
		End Function
		Friend Function InsertMultipleQuery(ByVal tableName As String, ByVal keys As String, ByVal values As List(Of String)) As String
			Dim ret As String = "BEGIN TRANSACTION;" & "  INSERT INTO " & PreparedTableName(tableName) & " " & "  (" & keys & ") " & "  VALUES "

			Dim added As Integer = 0
			For Each value As String In values
				If added > 0 Then
					ret &= ","
				End If
				ret &= "  (" & value & ")"
				added += 1
			Next value

			ret &= ";  COMMIT; "

			Return ret
		End Function

		Friend Function UpdateQuery(ByVal tableName As String, ByVal keyValueClause As String, ByVal filter As Expr) As String
			Dim ret As String = "UPDATE " & PreparedTableName(tableName) & " SET " & keyValueClause & " "

			If filter IsNot Nothing Then
				ret &= "WHERE " & ExpressionToWhereClause(filter) & " "
			End If
			ret &= "RETURNING *"

			Return ret
		End Function

		Friend Function DeleteQuery(ByVal tableName As String, ByVal filter As Expr) As String
			Dim ret As String = "DELETE FROM " & PreparedTableName(tableName) & " "

			If filter IsNot Nothing Then
				ret &= "WHERE " & ExpressionToWhereClause(filter) & " "
			End If

			Return ret
		End Function

		Friend Function TruncateQuery(ByVal tableName As String) As String
			Return "TRUNCATE TABLE " & PreparedTableName(tableName) & " "
		End Function

		Friend Function ExistsQuery(ByVal tableName As String, ByVal filter As Expr) As String
			Dim query As String = ""
			Dim whereClause As String = ""

			' select 
			query = "SELECT * " & "FROM " & PreparedTableName(tableName) & " "

			' expressions 
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			query &= "LIMIT 1"
			Return query
		End Function

		Friend Function CountQuery(ByVal tableName As String, ByVal countColumnName As String, ByVal filter As Expr) As String
			Dim query As String = ""
			Dim whereClause As String = ""

			' select 
			query = "SELECT COUNT(*) AS " & countColumnName & " " & "FROM " & PreparedTableName(tableName) & " "

			' expressions 
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			Return query
		End Function

		Friend Function SumQuery(ByVal tableName As String, ByVal fieldName As String, ByVal sumColumnName As String, ByVal filter As Expr) As String
			Dim whereClause As String = ""

			' select 
			Dim query As String = "SELECT SUM(" & SanitizeFieldname(fieldName) & ") AS " & sumColumnName & " " & "FROM " & PreparedTableName(tableName) & " "

			' expressions 
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			Return query
		End Function

		Friend Function PreparedFieldName(ByVal s As String) As String
			Return """" & s & """"
		End Function

		Friend Function PreparedStringValue(ByVal s As String) As String
			' uses $xx$ escaping
			Return PostgresqlHelper.SanitizeString(s)
		End Function

		Friend Function PreparedTableName(ByVal s As String) As String
			s = s.Replace("[", "")
			s = s.Replace("]", "")
			If s.Contains(".") Then
				Dim parts() As String = s.Split("."c)
				If parts.Length <> 2 Then
					Throw New ArgumentException("Table name must have either zero or one period '.' character")
				End If
				Return SanitizeStringInternal(parts(0)) & "." & SanitizeStringInternal(parts(1))
			Else
				Return SanitizeStringInternal(s)
			End If
		End Function

		Friend Function ExtractTableName(ByVal s As String) As String
			s = s.Replace("[", "")
			s = s.Replace("]", "")
			If s.Contains(".") Then
				Dim parts() As String = s.Split("."c)
				If parts.Length <> 2 Then
					Throw New ArgumentException("Table name must have either zero or one period '.' character")
				End If
				Return SanitizeStringInternal(parts(1))
			Else
				Return SanitizeStringInternal(s)
			End If
		End Function

		Friend Function PreparedUnicodeValue(ByVal s As String) As String
			Return "U&" & PreparedStringValue(s)
		End Function

		Friend Function ExpressionToWhereClause(ByVal expr As Expr) As String
			If expr Is Nothing Then
				Return Nothing
			End If

			Dim clause As String = ""

			If expr.Left Is Nothing Then
				Return Nothing
			End If

			clause &= "("

			If TypeOf expr.Left Is Expr Then
				clause &= ExpressionToWhereClause(CType(expr.Left, Expr)) & " "
			Else
				If Not (TypeOf expr.Left Is String) Then
					Throw New ArgumentException("Left term must be of type Expression or String")
				End If

				If expr.Operator <> OperatorEnum.Contains AndAlso expr.Operator <> OperatorEnum.ContainsNot AndAlso expr.Operator <> OperatorEnum.StartsWith AndAlso expr.Operator <> OperatorEnum.StartsWithNot AndAlso expr.Operator <> OperatorEnum.EndsWith AndAlso expr.Operator <> OperatorEnum.EndsWithNot Then
					'
					' These operators will add the left term
					'
					clause &= PreparedFieldName(expr.Left.ToString()) & " "
				End If
			End If

			Select Case expr.Operator
'				#Region "Process-By-Operators"

				Case OperatorEnum.And
'					#Region "And"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "AND "

					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.Or
'					#Region "Or"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "OR "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.Equals
'					#Region "Equals"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "= "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.NotEquals
'					#Region "NotEquals"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "<> "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.In
'					#Region "In"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					Dim inAdded As Integer = 0
					If Not Helper.IsList(expr.Right) Then
						Return Nothing
					End If
					Dim inTempList As List(Of Object) = Helper.ObjectToList(expr.Right)
					clause &= " IN ("
					For Each currObj As Object In inTempList
						If currObj Is Nothing Then
							Continue For
						End If
						If inAdded > 0 Then
							clause &= ","
						End If
						If TypeOf currObj Is DateTime OrElse TypeOf currObj Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(currObj)) & "'"
						ElseIf TypeOf currObj Is Integer OrElse TypeOf currObj Is Long OrElse TypeOf currObj Is Decimal Then
							clause &= currObj.ToString()
						Else
							clause &= PreparedStringValue(currObj.ToString())
						End If
						inAdded += 1
					Next currObj
					clause &= ")"

'				#End Region

				Case OperatorEnum.NotIn
'					#Region "NotIn"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					Dim notInAdded As Integer = 0
					If Not Helper.IsList(expr.Right) Then
						Return Nothing
					End If
					Dim notInTempList As List(Of Object) = Helper.ObjectToList(expr.Right)
					clause &= " NOT IN ("
					For Each currObj As Object In notInTempList
						If currObj Is Nothing Then
							Continue For
						End If
						If notInAdded > 0 Then
							clause &= ","
						End If
						If TypeOf currObj Is DateTime OrElse TypeOf currObj Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(currObj)) & "'"
						ElseIf TypeOf currObj Is Integer OrElse TypeOf currObj Is Long OrElse TypeOf currObj Is Decimal Then
							clause &= currObj.ToString()
						Else
							clause &= PreparedStringValue(currObj.ToString())
						End If
						notInAdded += 1
					Next currObj
					clause &= ")"

'				#End Region

				Case OperatorEnum.Contains
'					#Region "Contains"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					If TypeOf expr.Right Is String Then
						clause &= "(" & PreparedFieldName(expr.Left.ToString()) & " LIKE " & PreparedStringValue("%" & expr.Right.ToString()) & "OR " & PreparedFieldName(expr.Left.ToString()) & " LIKE " & PreparedStringValue("%" & expr.Right.ToString() & "%") & "OR " & PreparedFieldName(expr.Left.ToString()) & " LIKE " & PreparedStringValue(expr.Right.ToString() & "%") & ")"
					Else
						Return Nothing
					End If

'				#End Region

				Case OperatorEnum.ContainsNot
'					#Region "ContainsNot"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					If TypeOf expr.Right Is String Then
						clause &= "(" & PreparedFieldName(expr.Left.ToString()) & " NOT LIKE " & PreparedStringValue("%" & expr.Right.ToString()) & "OR " & PreparedFieldName(expr.Left.ToString()) & " NOT LIKE " & PreparedStringValue("%" & expr.Right.ToString() & "%") & "OR " & PreparedFieldName(expr.Left.ToString()) & " NOT LIKE " & PreparedStringValue(expr.Right.ToString() & "%") & ")"
					Else
						Return Nothing
					End If

'				#End Region

				Case OperatorEnum.StartsWith
'					#Region "StartsWith"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					If TypeOf expr.Right Is String Then
						clause &= "(" & PreparedFieldName(expr.Left.ToString()) & " LIKE " & PreparedStringValue(expr.Right.ToString() & "%") & ")"
					Else
						Return Nothing
					End If

'				#End Region

				Case OperatorEnum.StartsWithNot
'					#Region "StartsWithNot"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					If TypeOf expr.Right Is String Then
						clause &= "(" & PreparedFieldName(expr.Left.ToString()) & " NOT LIKE " & PreparedStringValue(expr.Right.ToString() & "%") & ")"
					Else
						Return Nothing
					End If

'				#End Region

				Case OperatorEnum.EndsWith
'					#Region "EndsWith"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					If TypeOf expr.Right Is String Then
						clause &= "(" & PreparedFieldName(expr.Left.ToString()) & " LIKE " & PreparedStringValue("%" & expr.Right.ToString()) & ")"
					Else
						Return Nothing
					End If

'				#End Region

				Case OperatorEnum.EndsWithNot
'					#Region "EndsWithNot"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					If TypeOf expr.Right Is String Then
						clause &= "(" & PreparedFieldName(expr.Left.ToString()) & " NOT LIKE " & PreparedStringValue("%" & expr.Right.ToString()) & ")"
					Else
						Return Nothing
					End If

'				#End Region

				Case OperatorEnum.GreaterThan
'					#Region "GreaterThan"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "> "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.GreaterThanOrEqualTo
'					#Region "GreaterThanOrEqualTo"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= ">= "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.LessThan
'					#Region "LessThan"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "< "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.LessThanOrEqualTo
'					#Region "LessThanOrEqualTo"

					If expr.Right Is Nothing Then
						Return Nothing
					End If
					clause &= "<= "
					If TypeOf expr.Right Is Expr Then
						clause &= ExpressionToWhereClause(CType(expr.Right, Expr))
					Else
						If TypeOf expr.Right Is DateTime OrElse TypeOf expr.Right Is DateTime? Then
							clause &= "'" & DbTimestamp(Convert.ToDateTime(expr.Right)) & "'"
						ElseIf TypeOf expr.Right Is Integer OrElse TypeOf expr.Right Is Long OrElse TypeOf expr.Right Is Decimal Then
							clause &= expr.Right.ToString()
						Else
							clause &= PreparedStringValue(expr.Right.ToString())
						End If
					End If

'				#End Region

				Case OperatorEnum.IsNull
'					#Region "IsNull"

					clause &= " IS NULL"

'				#End Region

				Case OperatorEnum.IsNotNull
'					#Region "IsNotNull"

					clause &= " IS NOT NULL"

'					#End Region

'					#End Region
			End Select

			clause &= ")"

			Return clause
		End Function

		Friend Function DbTimestamp(ByVal ts As DateTime) As String
			Return ts.ToString(TimestampFormat)
		End Function

		Friend Function DbTimestampOffset(ByVal ts As DateTimeOffset) As String
			Return ts.ToString(TimestampOffsetFormat)
		End Function

		Private Function BuildOrderByClause(ByVal resultOrder() As ResultOrder) As String
			If resultOrder Is Nothing OrElse resultOrder.Length < 0 Then
				Return Nothing
			End If

			Dim ret As String = "ORDER BY "

			For i As Integer = 0 To resultOrder.Length - 1
				If i > 0 Then
					ret &= ", "
				End If
				ret &= SanitizeFieldname(resultOrder(i).ColumnName) & " "
				If resultOrder(i).Direction = OrderDirection.Ascending Then
					ret &= "ASC"
				ElseIf resultOrder(i).Direction = OrderDirection.Descending Then
					ret &= "DESC"
				End If
			Next i

			ret &= " "
			Return ret
		End Function

		Private Function SanitizeStringInternal(ByVal val As String) As String
			Dim ret As String = ""

			'
			' null, below ASCII range, above ASCII range
			'
			For i As Integer = 0 To val.Length - 1
				If (AscW(val.Chars(i)) = 10) OrElse (AscW(val.Chars(i)) = 13) Then ' and line feed
					ret &= val.Chars(i)
				ElseIf AscW(val.Chars(i)) < 32 Then
					Continue For
				Else
					ret &= val.Chars(i)
				End If
			Next i

			'
			' double dash
			'
			Dim doubleDash As Integer = 0
			Do
				doubleDash = ret.IndexOf("--")
				If doubleDash < 0 Then
					Exit Do
				Else
					ret = ret.Remove(doubleDash, 2)
				End If
			Loop

			'
			' open comment
			' 
			Dim openComment As Integer = 0
			Do
				openComment = ret.IndexOf("/*")
				If openComment < 0 Then
					Exit Do
				Else
					ret = ret.Remove(openComment, 2)
				End If
			Loop

			'
			' close comment
			'
			Dim closeComment As Integer = 0
			Do
				closeComment = ret.IndexOf("*/")
				If closeComment < 0 Then
					Exit Do
				Else
					ret = ret.Remove(closeComment, 2)
				End If
			Loop

			'
			' in-string replacement
			'
			ret = ret.Replace("'", "''")
			Return ret
		End Function
	End Module
End Namespace
