Imports System
Imports System.Collections
Imports System.Collections.Generic
Imports System.Data
Imports System.Dynamic
Imports System.Linq
Imports System.Reflection
Imports System.Text
Imports System.Threading.Tasks

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Static helper methods for DatabaseWrapper.
	''' </summary>
	Public Class Helper
		''' <summary>
		''' Determines if an object is of a List type.
		''' </summary>
		''' <param name="o">Object.</param>
		''' <returns>True if the object is of a List type.</returns>
		Public Shared Function IsList(ByVal o As Object) As Boolean
			If o Is Nothing Then
				Return False
			End If
			Return TypeOf o Is IList AndAlso o.GetType().IsGenericType AndAlso o.GetType().GetGenericTypeDefinition().IsAssignableFrom(GetType(List(Of )))
		End Function

		''' <summary>
		''' Convert an object to a List object.
		''' </summary>
		''' <param name="obj">Object.</param>
		''' <returns>List object.</returns>
		Public Shared Function ObjectToList(ByVal obj As Object) As List(Of Object)
			If obj Is Nothing Then
				Return Nothing
			End If
			Dim ret As New List(Of Object)()
			Dim enumerator = DirectCast(obj, IEnumerable).GetEnumerator()
			Do While enumerator.MoveNext()
				ret.Add(enumerator.Current)
			Loop
			Return ret
		End Function

		''' <summary>
		''' Determine if a DataTable is null or has no rows.
		''' </summary>
		''' <param name="table">DataTable.</param>
		''' <returns>True if DataTable is null or has no rows.</returns>
		Public Shared Function DataTableIsNullOrEmpty(ByVal table As DataTable) As Boolean
			If table Is Nothing Then
				Return True
			End If
			If table.Rows.Count < 1 Then
				Return True
			End If
			Return False
		End Function

		''' <summary>
		''' Convert a DataTable to an object.
		''' </summary>
		''' <typeparam name="T">Type of object.</typeparam>
		''' <param name="table">DataTable.</param>
		''' <returns>Object of specified type.</returns>
		Public Shared Function DataTableToObject(Of T As New)(ByVal table As DataTable) As T
			If table Is Nothing Then
				Throw New ArgumentNullException(NameOf(table))
			End If
			If table.Rows.Count < 1 Then
				Throw New ArgumentException("No rows in DataTable")
			End If
			For Each r As DataRow In table.Rows
				Return DataRowToObject(Of T)(r)
			Next r
			Return CType(Nothing, T)
		End Function

		''' <summary>
		''' Convert a DataRow to an object.
		''' </summary>
		''' <typeparam name="T">Type of object.</typeparam>
		''' <param name="row">DataRow.</param>
		''' <returns>Object of specified type.</returns>
		Public Shared Function DataRowToObject(Of T As New)(ByVal row As DataRow) As T
			If row Is Nothing Then
				Throw New ArgumentNullException(NameOf(row))
			End If
			Dim item As New T()
			Dim properties As IList(Of PropertyInfo) = GetType(T).GetProperties().ToList()
			For Each [property] In properties
				[property].SetValue(item, row([property].Name), Nothing)
			Next [property]
			Return item
		End Function

		''' <summary>
		''' Convert a DataTable to a List of dynamic objects.
		''' </summary>
		''' <param name="table">DataTable.</param>
		''' <returns>List of dynamic objects.</returns>
		' NOTE: In the following line,  substituted 'Object' for 'dynamic' - this will work in VB with Option Strict Off:
		Public Shared Function DataTableToListDynamic(ByVal table As DataTable) As List(Of Object)
			' NOTE: In the following line,  substituted 'Object' for 'dynamic' - this will work in VB with Option Strict Off:
			Dim ret As New List(Of Object)()
			If table Is Nothing OrElse table.Rows.Count < 1 Then
				Return ret
			End If

			For Each curr As DataRow In table.Rows
				' NOTE: In the following line,  substituted 'Object' for 'dynamic' - this will work in VB with Option Strict Off:
				Dim dyn As Object = New ExpandoObject()
				For Each col As DataColumn In table.Columns
					Dim dic = DirectCast(dyn, IDictionary(Of String, Object))
					dic(col.ColumnName) = curr(col)
				Next col
				ret.Add(dyn)
			Next curr

			Return ret
		End Function

		''' <summary>
		''' Convert a DataTable to a dynamic object.
		''' </summary>
		''' <param name="table">DataTable.</param>
		''' <returns>Dynamic object.</returns>
		' NOTE: In the following line,  substituted 'Object' for 'dynamic' - this will work in VB with Option Strict Off:
		Public Shared Function DataTableToDynamic(ByVal table As DataTable) As Object
			' NOTE: In the following line,  substituted 'Object' for 'dynamic' - this will work in VB with Option Strict Off:
			Dim ret As Object = New ExpandoObject()
			If table Is Nothing OrElse table.Rows.Count < 1 Then
				Return ret
			End If
			If table.Rows.Count <> 1 Then
				Throw New ArgumentException("DataTable must contain only one row.")
			End If

			For Each curr As DataRow In table.Rows
				For Each col As DataColumn In table.Columns
					Dim dic = DirectCast(ret, IDictionary(Of String, Object))
					dic(col.ColumnName) = curr(col)
				Next col

				Return ret
			Next curr

			Return ret
		End Function

		''' <summary>
		''' Convert a DataTable to a List Dictionary.
		''' </summary>
		''' <param name="table">DataTable.</param>
		''' <returns>List Dictionary.</returns>
		Public Shared Function DataTableToListDictionary(ByVal table As DataTable) As List(Of Dictionary(Of String, Object))
			Dim ret As New List(Of Dictionary(Of String, Object))()
			If table Is Nothing OrElse table.Rows.Count < 1 Then
				Return ret
			End If

			For Each curr As DataRow In table.Rows
				Dim currDict As New Dictionary(Of String, Object)()

				For Each col As DataColumn In table.Columns
					currDict.Add(col.ColumnName, curr(col))
				Next col

				ret.Add(currDict)
			Next curr

			Return ret
		End Function

		''' <summary>
		''' Convert a DataTable to a Dictionary.
		''' </summary>
		''' <param name="table">DataTable.</param>
		''' <returns>Dictionary.</returns>
		Public Shared Function DataTableToDictionary(ByVal table As DataTable) As Dictionary(Of String, Object)
			Dim ret As New Dictionary(Of String, Object)()
			If table Is Nothing OrElse table.Rows.Count < 1 Then
				Return ret
			End If
			If table.Rows.Count <> 1 Then
				Throw New ArgumentException("DataTable must contain only one row.")
			End If

			For Each curr As DataRow In table.Rows
				For Each col As DataColumn In table.Columns
					ret.Add(col.ColumnName, curr(col))
				Next col

				Return ret
			Next curr

			Return ret
		End Function

		''' <summary>
		''' Determine if string contains extended characters.
		''' </summary>
		''' <param name="data">String.</param>
		''' <returns>True if string contains extended characters.</returns>
		Public Shared Function IsExtendedCharacters(ByVal data As String) As Boolean
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
		''' Retrieve the DataType from the column type.
		''' </summary>
		''' <param name="s">String containing column type.</param>
		''' <returns>DataType.</returns>
		Public Shared Function DataTypeFromString(ByVal s As String) As DataType
			If String.IsNullOrEmpty(s) Then
				Throw New ArgumentNullException(NameOf(s))
			End If

			s = s.ToLower()
			If s.Contains("(") Then
				s = s.Substring(0, s.IndexOf("("))
			End If

			Select Case s
				Case "bigserial", "bigint" ' pgsql
					Return DataType.Long

				Case "boolean", "bit", "smallserial", "smallest", "tinyint", "integer", "int", "smallint", "mediumint", "serial" ' pgsql
					Return DataType.Int

				Case "real", "double", "double precision", "float" ' pgsql, sqlite
					Return DataType.Double

				Case "decimal", "numeric" ' mssql
					Return DataType.Decimal

				Case "timestamp without timezone", "timestamp without time zone", "time without timezone", "time without time zone", "time", "date", "datetime", "datetime2", "timestamp" ' pgsql
					Return DataType.DateTime

				Case "time with timezone", "time with time zone", "timestamp with timezone", "timestamp with time zone", "datetimeoffset" ' pgsql
					Return DataType.DateTimeOffset

				Case "enum", "character", "char", "text", "varchar" ' mysql
					Return DataType.Varchar

				Case "character varying", "nchar", "ntext", "nvarchar" ' pgsql
					Return DataType.Nvarchar ' mssql

				Case "blob", "tinyblob", "mediumblob", "longblob", "bytea", "varbinary" ' sqlite, mysql
					Return DataType.Blob

				Case Else
					Throw New ArgumentException("Unknown DataType: " & s)
			End Select
		End Function

		''' <summary>
		''' Convert byte array to hex string.
		''' </summary>
		''' <param name="bytes">Byte array.</param>
		''' <returns>Hex string.</returns>
		Public Shared Function ByteArrayToHexString(ByVal bytes() As Byte) As String
			If bytes Is Nothing Then
				Return Nothing
			End If
			If bytes.Length < 1 Then
				Return ""
			End If
			Dim hex As New StringBuilder(bytes.Length * 2)
			For Each b As Byte In bytes
				hex.AppendFormat("{0:x2}", b)
			Next b
			Return hex.ToString()
		End Function

		''' <summary>
		''' Convert hex string to byte array.
		''' </summary>
		''' <param name="hex">Hex string.</param>
		''' <returns>Byte array.</returns>
		Public Shared Function HexStringToBytes(ByVal hex As String) As Byte()
#Enable Warning BCCS1591 ' Missing XML comment for publicly visible type or member
			If String.IsNullOrEmpty(hex) Then
				Throw New ArgumentNullException(NameOf(hex))
			End If
			If hex.Length Mod 2 = 1 Then
				Throw New ArgumentException("The supplied hex cannot have an odd number of digits.")
			End If

			Dim arr((hex.Length >> 1) - 1) As Byte

			Dim i As Integer = 0
			'Do While i(Of hex.Length) > 1
			'	arr(i) = CByte((GetHexValue(hex.Chars(i << 1)) << 4) + (GetHexValue(hex.Chars((i << 1) + 1))))
			'	i += 1
			'Loop
			For i = 0 To i < hex.Length >> 1 Step +1
				arr(i) = CType(((GetHexValue(hex(i < 1)) < 4) + (GetHexValue(hex((i < 1) + 1)))), Byte)
			Next
			Return arr
		End Function

		Private Shared Function GetHexValue(ByVal hex As Char) As Integer
			Dim val As Integer = AscW(hex)
			' For uppercase A-F letters:
			' return val - (val < 58 ? 48 : 55);
			' For lowercase a-f letters:
			' return val - (val < 58 ? 48 : 87);
			' Or the two combined, but a bit slower:
			Return val - (If(val < 58, 48, (If(val < 97, 55, 87))))
		End Function
	End Class
End Namespace
