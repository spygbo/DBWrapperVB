Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports System.Runtime.Serialization

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Type of data contained in the column.
	''' </summary>
	Public Enum DataType
		''' <summary>
		''' Variable-length character.
		''' </summary>
		<EnumMember(Value := "Varchar")>
		Varchar
		''' <summary>
		''' Variable-length unicode character.
		''' </summary>
		<EnumMember(Value := "Nvarchar")>
		Nvarchar
		''' <summary>
		''' Integer.
		''' </summary>
		<EnumMember(Value := "Int")>
		Int
		''' <summary>
		''' Long
		''' </summary>
		<EnumMember(Value := "Long")>
		[Long]
		''' <summary>
		''' Decimal
		''' </summary>
		<EnumMember(Value := "Decimal")>
		[Decimal]
		''' <summary>
		''' Double
		''' </summary>
		<EnumMember(Value := "Double")>
		[Double]
		''' <summary>
		''' Timestamp
		''' </summary>
		<EnumMember(Value := "DateTime")>
		DateTime
		''' <summary>
		''' Timestamp with offset.
		''' </summary>
		<EnumMember(Value := "DateTimeOffset")>
		DateTimeOffset
		''' <summary>
		''' Blob
		''' </summary>
		<EnumMember(Value := "Blob")>
		Blob
	End Enum
End Namespace