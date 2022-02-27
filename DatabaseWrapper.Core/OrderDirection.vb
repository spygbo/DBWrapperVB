Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Text
Imports System.Threading.Tasks
Imports System.Runtime.Serialization

Namespace DatabaseWrapper.Core
	''' <summary>
	''' Direction by which results should be returned.
	''' </summary>
	Public Enum OrderDirection
		''' <summary>
		''' Return results in ascending order.
		''' </summary>
		<EnumMember(Value := "Ascending")>
		Ascending
		''' <summary>
		''' Return results in descending order.
		''' </summary>
		<EnumMember(Value := "Descending")>
		Descending
	End Enum
End Namespace