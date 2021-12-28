package com.nec.ve

import com.nec.ve.CVector.CScalarVector

sealed trait VeType {
  def containerSize: Int
  def isString: Boolean
  def cVectorType: String
  def makeCVector(name: String): CVector
}

object VeType {
  val All: Set[VeType] = Set(VeString) ++ VeScalarType.All

  case object VeString extends VeType {
    override def cVectorType: String = "nullable_varchar_vector"

    override def makeCVector(name: String): CVector = CVector.varChar(name)

    override def isString: Boolean = true

    override def containerSize: Int = 32
  }

  sealed trait VeScalarType extends VeType {
    override def containerSize: Int = 20

    def cScalarType: String

    def cSize: Int

    override def makeCVector(name: String): CVector = CScalarVector(name, this)

    override def isString: Boolean = false
  }

  object VeScalarType {
    val All: Set[VeScalarType] =
      Set(VeNullableDouble, VeNullableFloat, VeNullableInt, VeNullableLong)

    case object VeNullableDouble extends VeScalarType {

      def cScalarType: String = "double"

      def cVectorType: String = "nullable_double_vector"

      override def cSize: Int = 8
    }

    case object VeNullableFloat extends VeScalarType {
      def cScalarType: String = "float"

      def cVectorType: String = "nullable_float_vector"

      override def cSize: Int = 4
    }

    case object VeNullableInt extends VeScalarType {
      def cScalarType: String = "int32_t"

      def cVectorType: String = "nullable_int_vector"

      override def cSize: Int = 4
    }

    case object VeNullableLong extends VeScalarType {
      def cScalarType: String = "int64_t"

      def cVectorType: String = "nullable_bigint_vector"

      override def cSize: Int = 8
    }

    def veNullableDouble: VeScalarType = VeNullableDouble

    def veNullableInt: VeScalarType = VeNullableInt

    def veNullableLong: VeScalarType = VeNullableLong
  }
}
