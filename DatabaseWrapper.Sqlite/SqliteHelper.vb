Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports DatabaseWrapper.Core
Imports ExpressionTree

Namespace DatabaseWrapper.Sqlite
	Friend Module SqliteHelper
		Friend TimestampFormat As String = "yyyy-MM-dd HH:mm:ss.ffffff"

		Friend TimestampOffsetFormat As String = "yyyy-MM-dd HH:mm:ss.fffffff zzz"

		Friend Function ConnectionString(ByVal settings As DatabaseSettings) As String
			Return "Data Source=" & settings.Filename
		End Function

		Friend Function LoadTableNamesQuery() As String
			Return "DROP TABLE IF EXISTS sqlitemetadata; " & "CREATE TEMPORARY TABLE sqlitemetadata AS " & "  SELECT " & "    name AS TABLE_NAME " & "  FROM " & "    sqlite_master " & "  WHERE " & "    type ='table' " & "    AND name NOT LIKE 'sqlite_%'; " & "SELECT * FROM sqlitemetadata;"
		End Function

		Friend Function LoadTableColumnsQuery(ByVal table As String) As String
			Return "DROP TABLE IF EXISTS sqlitemetadata; " & "CREATE TEMPORARY TABLE sqlitemetadata AS " & "  SELECT " & "    m.name AS TABLE_NAME,  " & "    p.name AS COLUMN_NAME, " & "    p.type AS DATA_TYPE, " & "    p.pk AS IS_PRIMARY_KEY, " & "    p.[notnull] AS IS_NOT_NULLABLE " & "  FROM " & "    sqlite_master m " & "  LEFT OUTER JOIN pragma_table_info((m.name)) p " & "    ON m.name <> p.name " & "  WHERE " & "    m.type = 'table' " & "    AND m.name = '" & table & "' " & "  ORDER BY TABLE_NAME; " & "SELECT * FROM sqlitemetadata; "
		End Function

		Friend Function SanitizeString(ByVal val As String) As String
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
			Dim ret As String = PreparedFieldName(SanitizeString(col.Name)) & " "

			Select Case col.Type
				Case DataType.Varchar, DataType.Nvarchar
					ret &= "VARCHAR(" & col.MaxLength & ") COLLATE NOCASE "
				Case DataType.Int
					ret &= "INTEGER "
				Case DataType.Long
					ret &= "BIGINT "
				Case DataType.Decimal
					ret &= "DECIMAL(" & col.MaxLength & "," & col.Precision & ") "
				Case DataType.Double
					ret &= "REAL "
				Case DataType.DateTime, DataType.DateTimeOffset
					ret &= "TEXT "
				Case DataType.Blob
					ret &= "BLOB "
				Case Else
					Throw New ArgumentException("Unknown DataType: " & col.Type.ToString())
			End Select

			If col.PrimaryKey Then
				If col.Type = DataType.Varchar OrElse col.Type = DataType.Nvarchar Then
					ret &= "UNIQUE "
				ElseIf col.Type = DataType.Int OrElse col.Type = DataType.Long Then
					ret &= "PRIMARY KEY AUTOINCREMENT "
				Else
					Throw New ArgumentException("Primary key column '" & col.Name & "' is of an unsupported type: " & col.Type.ToString())
				End If
			End If

			If Not col.Nullable Then
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
			Dim ret As String = "CREATE TABLE IF NOT EXISTS " & PreparedFieldName(SanitizeString(tableName)) & " " & "("

			Dim added As Integer = 0
			For Each curr As Column In columns
				If added > 0 Then
					ret &= ", "
				End If
				ret &= ColumnToCreateString(curr)
				added += 1
			Next curr

			ret &= ")"

			Return ret
		End Function

		Friend Function DropTableQuery(ByVal tableName As String) As String
			Return "DROP TABLE IF EXISTS " & PreparedFieldName(SanitizeString(tableName))
		End Function

		Friend Function SelectQuery(ByVal tableName As String, ByVal indexStart? As Integer, ByVal maxResults? As Integer, ByVal returnFields As List(Of String), ByVal filter As Expr, ByVal resultOrder() As ResultOrder) As String
			Dim query As String = ""
			Dim whereClause As String = ""

			'
			' select
			'
			query = "SELECT "

			'
			' fields
			'
			If returnFields Is Nothing OrElse returnFields.Count < 1 Then
				query &= "* "
			Else
				Dim fieldsAdded As Integer = 0
				For Each curr As String In returnFields
					If fieldsAdded = 0 Then
						query &= PreparedFieldName(SanitizeString(curr))
						fieldsAdded += 1
					Else
						query &= "," & PreparedFieldName(SanitizeString(curr))
						fieldsAdded += 1
					End If
				Next curr
			End If
			query &= " "

			'
			' table
			'
			query &= "FROM " & PreparedFieldName(SanitizeString(tableName)) & " "

			'
			' expressions
			'
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			' 
			' order clause
			' 
			query &= BuildOrderByClause(resultOrder)

			'
			' pagination
			'
			If indexStart IsNot Nothing AndAlso maxResults IsNot Nothing Then
				query &= "LIMIT " & maxResults & " "
				query &= "OFFSET " & indexStart & " "
			ElseIf maxResults IsNot Nothing Then
				query &= "LIMIT " & maxResults & " "
			End If

			Return query
		End Function

		Friend Function InsertQuery(ByVal tableName As String, ByVal keys As String, ByVal values As String) As String
			Return "BEGIN TRANSACTION; " & "INSERT INTO " & PreparedFieldName(SanitizeString(tableName)) & " " & "(" & keys & ") " & "VALUES " & "(" & values & "); " & "SELECT last_insert_rowid() AS id; " & ";COMMIT;"
		End Function

		Friend Function InsertMultipleQuery(ByVal tableName As String, ByVal keys As String, ByVal values As List(Of String)) As String
			Dim ret As String = "BEGIN TRANSACTION; " & "  INSERT INTO " & PreparedFieldName(SanitizeString(tableName)) & " " & "  (" & keys & ") " & "  VALUES "

			Dim added As Integer = 0
			For Each value As String In values
				If added > 0 Then
					ret &= ","
				End If
				ret &= "  (" & value & ")"
				added += 1
			Next value

			ret &= ";COMMIT;"

			Return ret
		End Function

		Friend Function UpdateQuery(ByVal tableName As String, ByVal keyValueClause As String, ByVal filter As Expr) As String
			Dim ret As String = "BEGIN TRANSACTION; " & "UPDATE " & PreparedFieldName(SanitizeString(tableName)) & " SET " & keyValueClause & " "

			If filter IsNot Nothing Then
				ret &= "WHERE " & ExpressionToWhereClause(filter) & " "
			End If

			ret &= ";COMMIT;"

			Return ret
		End Function

		Friend Function DeleteQuery(ByVal tableName As String, ByVal filter As Expr) As String
			Dim ret As String = "DELETE FROM " & PreparedFieldName(SanitizeString(tableName)) & " "

			If filter IsNot Nothing Then
				ret &= "WHERE " & ExpressionToWhereClause(filter) & " "
			End If

			Return ret
		End Function

		Friend Function TruncateQuery(ByVal tableName As String) As String
			Return "DELETE FROM " & PreparedFieldName(SanitizeString(tableName))
		End Function

		Friend Function ExistsQuery(ByVal tableName As String, ByVal filter As Expr) As String
			Dim query As String = ""
			Dim whereClause As String = ""

			'
			' select
			'
			query = "SELECT * " & "FROM " & PreparedFieldName(SanitizeString(tableName)) & " "

			'
			' expressions
			'
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

			'
			' select
			'
			query = "SELECT COUNT(*) AS " & countColumnName & " " & "FROM " & PreparedFieldName(SanitizeString(tableName)) & " "

			'
			' expressions
			'
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			Return query
		End Function

		Friend Function SumQuery(ByVal tableName As String, ByVal fieldName As String, ByVal sumColumnName As String, ByVal filter As Expr) As String
			Dim query As String = ""
			Dim whereClause As String = ""

			'
			' select
			'
			query = "SELECT SUM(" & SanitizeString(fieldName) & ") AS " & sumColumnName & " " & "FROM " & PreparedFieldName(SanitizeString(tableName)) & " "

			'
			' expressions
			'
			If filter IsNot Nothing Then
				whereClause = ExpressionToWhereClause(filter)
			End If
			If Not String.IsNullOrEmpty(whereClause) Then
				query &= "WHERE " & whereClause & " "
			End If

			Return query
		End Function

		Friend Function DbTimestamp(ByVal ts As DateTime) As String
			Return ts.ToString(TimestampFormat)
		End Function

		Friend Function DbTimestampOffset(ByVal ts As DateTimeOffset) As String
			Return ts.ToString(TimestampOffsetFormat)
		End Function

		Friend Function PreparedFieldName(ByVal s As String) As String
			Return "`" & s & "`"
		End Function

		Friend Function PreparedStringValue(ByVal s As String) As String
			Return "'" & SqliteHelper.SanitizeString(s) & "'"
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
'					#Region "EndsWith"

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

		Private Function BuildOrderByClause(ByVal resultOrder() As ResultOrder) As String
			If resultOrder Is Nothing OrElse resultOrder.Length < 0 Then
				Return Nothing
			End If

			Dim ret As String = "ORDER BY "

			For i As Integer = 0 To resultOrder.Length - 1
				If i > 0 Then
					ret &= ", "
				End If
				ret &= SanitizeString(resultOrder(i).ColumnName) & " "
				If resultOrder(i).Direction = OrderDirection.Ascending Then
					ret &= "ASC"
				ElseIf resultOrder(i).Direction = OrderDirection.Descending Then
					ret &= "DESC"
				End If
			Next i

			ret &= " "
			Return ret
		End Function
	End Module
End Namespace
