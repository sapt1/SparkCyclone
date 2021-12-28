package com.nec.ve

import com.nec.spark.agile.CFunctionGeneration.CExpression
import com.nec.ve.CVector.CScalarVector
import com.nec.ve.VeType.VeScalarType

trait Aggregation extends Serializable {
  def merge(prefix: String, inputPrefix: String): CodeLines
  def initial(prefix: String): CodeLines
  def partialValues(prefix: String): List[(CScalarVector, CExpression)]
  def iterate(prefix: String): CodeLines
  def compute(prefix: String): CodeLines
  def fetch(prefix: String): CExpression
  def free(prefix: String): CodeLines
}
object Aggregation {

  final case class SuffixedAggregation(suffix: String, original: Aggregation) extends Aggregation {
    override def initial(prefix: String): CodeLines = original.initial(s"$prefix$suffix")

    override def iterate(prefix: String): CodeLines = original.iterate(s"$prefix$suffix")

    override def compute(prefix: String): CodeLines = original.compute(s"$prefix$suffix")

    override def fetch(prefix: String): CExpression = original.fetch(s"$prefix$suffix")

    override def free(prefix: String): CodeLines = original.free(s"$prefix$suffix")

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      original.partialValues(s"$prefix$suffix")

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      original.merge(s"$prefix$suffix", s"$inputPrefix$suffix")
  }

  abstract class DelegatingAggregation(val original: Aggregation) extends Aggregation {
    override def initial(prefix: String): CodeLines = original.initial(prefix)

    override def iterate(prefix: String): CodeLines = original.iterate(prefix)

    override def compute(prefix: String): CodeLines = original.compute(prefix)

    override def fetch(prefix: String): CExpression = original.fetch(prefix)

    override def free(prefix: String): CodeLines = original.free(prefix)

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      original.partialValues(prefix)

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      original.merge(prefix, inputPrefix)
  }

  def sum(cExpression: CExpression): Aggregation = new Aggregation {
    override def initial(prefix: String): CodeLines =
      CodeLines.from(s"double ${prefix}_aggregate_sum = 0;")

    override def iterate(prefix: String): CodeLines =
      cExpression.isNotNullCode match {
        case None =>
          CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};")
        case Some(notNullCheck) =>
          CodeLines.from(
            s"if ( ${notNullCheck} ) {",
            CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};").indented,
            "}"
          )
      }

    override def fetch(prefix: String): CExpression =
      CExpression(s"${prefix}_aggregate_sum", None)

    override def free(prefix: String): CodeLines = CodeLines.empty

    override def compute(prefix: String): CodeLines = CodeLines.empty

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      List(
        (
          CScalarVector(s"${prefix}_x", VeScalarType.veNullableDouble),
          CExpression(s"${prefix}_aggregate_sum", None)
        )
      )

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      CodeLines.from(s"${prefix}_aggregate_sum += ${inputPrefix}_x->data[i];")
  }
  def avg(cExpression: CExpression): Aggregation = new Aggregation {
    override def initial(prefix: String): CodeLines =
      CodeLines.from(s"double ${prefix}_aggregate_sum = 0;", s"long ${prefix}_aggregate_count = 0;")

    override def iterate(prefix: String): CodeLines =
      cExpression.isNotNullCode match {
        case None =>
          CodeLines.from(
            s"${prefix}_aggregate_sum += ${cExpression.cCode};",
            s"${prefix}_aggregate_count += 1;"
          )
        case Some(notNullCheck) =>
          CodeLines.from(
            s"if ( ${notNullCheck} ) {",
            CodeLines
              .from(
                s"${prefix}_aggregate_sum += ${cExpression.cCode};",
                s"${prefix}_aggregate_count += 1;"
              )
              .indented,
            "}"
          )
      }

    override def fetch(prefix: String): CExpression =
      CExpression(s"${prefix}_aggregate_sum / ${prefix}_aggregate_count", None)

    override def free(prefix: String): CodeLines = CodeLines.empty

    override def compute(prefix: String): CodeLines = CodeLines.empty

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] = List(
      (
        CScalarVector(s"${prefix}_aggregate_sum_partial_output", VeScalarType.veNullableDouble),
        CExpression(s"${prefix}_aggregate_sum", None)
      ),
      (
        CScalarVector(s"${prefix}_aggregate_count_partial_output", VeScalarType.veNullableLong),
        CExpression(s"${prefix}_aggregate_count", None)
      )
    )

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      CodeLines.from(
        s"${prefix}_aggregate_sum += ${inputPrefix}_aggregate_sum_partial_output->data[i];",
        s"${prefix}_aggregate_count += ${inputPrefix}_aggregate_count_partial_output->data[i];"
      )
  }
}
