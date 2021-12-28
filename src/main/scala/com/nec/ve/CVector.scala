package com.nec.ve

import com.nec.ve.VeType.{VeScalarType, VeString}

sealed trait CVector {
  def replaceName(search: String, replacement: String): CVector
  def name: String
  def veType: VeType
}
object CVector {
  def varChar(name: String): CVector = CVarChar(name)

  def double(name: String): CVector = CScalarVector(name, VeScalarType.veNullableDouble)

  def int(name: String): CVector = CScalarVector(name, VeScalarType.veNullableInt)

  final case class CVarChar(name: String) extends CVector {
    override def veType: VeType = VeString

    override def replaceName(search: String, replacement: String): CVector =
      copy(name = name.replaceAllLiterally(search, replacement))
  }

  final case class CScalarVector(name: String, veType: VeScalarType) extends CVector {
    override def replaceName(search: String, replacement: String): CVector =
      copy(name = name.replaceAllLiterally(search, replacement))
  }
}
