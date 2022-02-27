Imports System
Imports System.Collections.Generic
Imports System.Text

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Sanitization methods.
	''' </summary>
	Public Module Sanitizer
		''' <summary>
		''' SQL Server sanitizer.
		''' </summary>
		''' <param name="val">String.</param>
		''' <returns>Sanitized string.</returns>
		Public Function SqlServerSanitizer(ByVal val As String) As String
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

		''' <summary>
		''' MySQL sanitizer.
		''' </summary>
		''' <param name="val">String.</param>
		''' <returns>Sanitized string.</returns>
		Public Function MysqlSanitizer(ByVal val As String) As String
			Return SqlServerSanitizer(val)
		End Function

		''' <summary>
		''' PostgreSQL sanitizer.
		''' </summary>
		''' <param name="val">String.</param>
		''' <returns>Sanitized string.</returns>
		Public Function PostgresqlSanitizer(ByVal val As String) As String
			Dim tag As String = "$" & PostgresqlEscapeString(val, 2) & "$"
			Return tag & val & tag
		End Function

		Private Function PostgresqlEscapeString(ByVal val As String, ByVal numChar As Integer) As String
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

		''' <summary>
		''' Sqlite sanitizer.
		''' </summary>
		''' <param name="val">String.</param>
		''' <returns>Sanitized string.</returns>
		Public Function SqliteSanitizer(ByVal val As String) As String
			Return SqlServerSanitizer(val)
		End Function
	End Module
End Namespace
