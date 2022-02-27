Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Database table column.
	''' </summary>
	Public Class Column
		#Region "Public-Members"

		''' <summary>
		''' The name of the column.
		''' </summary>
		Public Name As String = Nothing

		''' <summary>
		''' Whether or not the column is the table's primary key.
		''' </summary>
		Public PrimaryKey As Boolean = False

		''' <summary>
		''' The data type of the column.
		''' </summary>
		Public Type As DataType = DataType.Varchar

		''' <summary>
		''' The maximum character length of the data contained within the column.
		''' </summary>
		Public MaxLength? As Integer = Nothing

		''' <summary>
		''' For precision, i.e. number of places after the decimal.
		''' </summary>
		Public Precision? As Integer = Nothing

		''' <summary>
		''' Whether or not the column can contain NULL.
		''' </summary>
		Public Nullable As Boolean = True

		#End Region

		#Region "Private-Members"

		Private _RequiresLengthAndPrecision As New List(Of DataType) From {DataType.Decimal, DataType.Double}

		Private _RequiresLength As New List(Of DataType) From {DataType.Nvarchar, DataType.Varchar}

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
		''' <param name="name">Name of the column.</param>
		''' <param name="primaryKey">Indicate if this column is the primary key.</param>
		''' <param name="dt">DataType for the column.</param>
		''' <param name="nullable">Indicate if this column is nullable.</param>
		Public Sub New(ByVal name As String, ByVal primaryKey As Boolean, ByVal dt As DataType, ByVal nullable As Boolean)
			If String.IsNullOrEmpty(name) Then
				Throw New ArgumentNullException(NameOf(name))
			End If
			If primaryKey AndAlso nullable Then
				Throw New ArgumentException("Primary key column '" & name & "' cannot be nullable.")
			End If

			Me.Name = name
			Me.PrimaryKey = primaryKey
			Type = dt
			Me.Nullable = nullable

			If _RequiresLengthAndPrecision.Contains(dt) Then
				Throw New ArgumentException("Column '" & name & "' must include both maximum length and precision; use the constructor that allows these values to be specified.")
			End If

			If _RequiresLength.Contains(dt) Then
				Throw New ArgumentException("Column '" & name & "' must include a maximum length; use the constructor that allows these values to be specified.")
			End If
		End Sub

		''' <summary>
		''' Instantiate the object.
		''' </summary>
		''' <param name="name">Name of the column.</param>
		''' <param name="primaryKey">Indicate if this column is the primary key.</param>
		''' <param name="dt">DataType for the column.</param>
		''' <param name="maxLen">Max length for the column.</param>
		''' <param name="precision">Precision for the column.</param>
		''' <param name="nullable">Indicate if this column is nullable.</param>
		Public Sub New(ByVal name As String, ByVal primaryKey As Boolean, ByVal dt As DataType, ByVal maxLen? As Integer, ByVal precision? As Integer, ByVal nullable As Boolean)
			If String.IsNullOrEmpty(name) Then
				Throw New ArgumentNullException(NameOf(name))
			End If
			If primaryKey AndAlso nullable Then
				Throw New ArgumentException("Primary key column '" & name & "' cannot be nullable.")
			End If
			' WARNING: Comparisons involving nullable type instances require Option Strict Off:
			If maxLen IsNot Nothing AndAlso maxLen < 1 Then
				Throw New ArgumentException("Column '" & name & "' maximum length must be greater than zero if not null.")
			End If
			' WARNING: Comparisons involving nullable type instances require Option Strict Off:
			If precision IsNot Nothing AndAlso precision < 1 Then
				Throw New ArgumentException("Column '" & name & "' preicision must be greater than zero if not null.")
			End If

			Me.Name = name
			Me.PrimaryKey = primaryKey
			Type = dt
			MaxLength = maxLen
			Me.Precision = precision
			Me.Nullable = nullable

			If _RequiresLengthAndPrecision.Contains(dt) Then
				' WARNING: Comparisons involving nullable type instances require Option Strict Off:
				If maxLen Is Nothing OrElse precision Is Nothing OrElse maxLen < 1 OrElse precision < 1 Then
					Throw New ArgumentException("Column '" & name & "' must include both maximum length and precision, and both must be greater than zero.")
				End If
			End If

			If _RequiresLength.Contains(dt) Then
				' WARNING: Comparisons involving nullable type instances require Option Strict Off:
				If maxLen Is Nothing OrElse maxLen < 1 Then
					Throw New ArgumentException("Column '" & name & "' must include a maximum length, and both must be greater than zero.")
				End If
			End If
		End Sub

		#End Region

		#Region "Public-Methods"

		''' <summary>
		''' Produce a human-readable string of the object.
		''' </summary>
		''' <returns>String.</returns>
		Public Overrides Function ToString() As String
			Dim ret As String = " [Column " & Name & "] "

			If PrimaryKey Then
				ret &= "PK "
			End If
			ret &= "Type: " & Type.ToString() & " "
			If MaxLength IsNot Nothing Then
				ret &= "MaxLen: " & MaxLength & " "
			End If
			If Precision IsNot Nothing Then
				ret &= "Precision: " & Precision & " "
			End If
			ret &= "Nullable: " & Nullable

			Return ret
		End Function

		#End Region

		#Region "Private-Methods"

		#End Region
	End Class
End Namespace
