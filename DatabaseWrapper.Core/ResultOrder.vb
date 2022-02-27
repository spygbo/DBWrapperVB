Imports System
Imports System.Collections.Generic
Imports System.Text

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Describe on which columns and in which direction results should be ordered.
	''' </summary>
	Public Class ResultOrder
		#Region "Public-Members"

		''' <summary>
		''' Column name on which to order results.
		''' </summary>
		Public Property ColumnName() As String = Nothing

		''' <summary>
		''' Direction by which results should be returned.
		''' </summary>
		Public Property Direction() As OrderDirection = OrderDirection.Ascending

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
		''' Instantiate the object.
		''' </summary>
		''' <param name="columnName">Column name on which to order results.</param>
		''' <param name="direction">Direction by which results should be returned.</param>
		Public Sub New(ByVal columnName As String, ByVal direction As OrderDirection)
			If String.IsNullOrEmpty(columnName) Then
				Throw New ArgumentNullException(NameOf(columnName))
			End If
			Me.ColumnName = columnName
			Me.Direction = direction
		End Sub

		#End Region

		#Region "Public-Methods"

		#End Region

		#Region "Private-Methods"

		#End Region
	End Class
End Namespace
