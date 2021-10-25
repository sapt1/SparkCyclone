package com.nec.spark.planning

import java.util.UUID

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.language.dynamics

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.cmake.ScalaUdpDebug
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.agile.{CFunctionGeneration, SparkExpressionToCExpression}
import com.nec.spark.planning.NativeAggregationEvaluationPlan.EvaluationMode.{
  PrePartitioned,
  TwoStaged
}
import com.nec.spark.planning.NativeAggregationEvaluationPlan.{writeVector, EvaluationMode}
import com.nec.spark.planning.NativeSortEvaluationPlan.SortingMode
import com.nec.spark.planning.NativeSortEvaluationPlan.SortingMode.Coalesced
import com.nec.spark.planning.Tracer.DefineTracer
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
object NativeSortEvaluationPlan {

  sealed trait SortingMode extends Serializable
  object SortingMode {
    final case class Coalesced(cFunction: CFunction) extends SortingMode
  }
}
//noinspection DuplicatedCode
final case class NativeSortEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  functionPrefix: String,
  sortingMode: SortingMode,
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = SinglePartition

  def collectInputRows(
    rows: Iterator[InternalRow],
    arrowSchema: org.apache.arrow.vector.types.pojo.Schema
  )(implicit allocator: BufferAllocator): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    rows.foreach { row =>
      arrowWriter.write(row)
    }
    arrowWriter.finish()
    root
  }

  def collectInputColBatches(columnarBatches: Iterator[ColumnarBatch], target: List[FieldVector])(
    implicit allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val root = new VectorSchemaRoot(target.asJava)
    val arrowWriter = ArrowWriter.create(root)

    for {
      columnarBatch <- columnarBatches
      i <- 0 until columnarBatch.numRows()
    } arrowWriter.write(columnarBatch.getRow(i))

    arrowWriter.finish()
    root
  }

  def collectInputRows(inputRows: Iterator[InternalRow], target: List[FieldVector])(implicit
    allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val root = new VectorSchemaRoot(target.asJava)
    val arrowWriter = ArrowWriter.create(root)
    inputRows.foreach(row => arrowWriter.write(row))

    arrowWriter.finish()
    root
  }

  private def coalesceAndExecute(coalesced: Coalesced): RDD[InternalRow] = {

    val evaluator = nativeEvaluator.forCode(
      coalesced.cFunction.toCodeLines(functionPrefix).lines.mkString("\n", "\n", "\n")
    )

    logger.debug(s"Will execute NewCEvaluationPlan for child ${child}; ${child.output}")

    child
      .execute()
      .coalesce(1)
      .mapPartitions { rows =>
        implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)
        val timeZoneId = conf.sessionLocalTimeZone
        val root =
          collectInputRows(rows, ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId))
        val inputVectors = child.output.indices.map(root.getVector)
        val outputVectors: List[ValueVector] =
          coalesced.cFunction.outputs.map(CFunctionGeneration.allocateFrom(_))

        try {

          val outputArgs = inputVectors.toList.map(_ => None) ++
            outputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
          val inputArgs = inputVectors.toList
            .map(iv => Some(SupportedVectorWrapper.wrapInput(iv))) ++ outputVectors.map(_ => None)

          evaluator.callFunction(
            name = functionPrefix,
            inputArguments = inputArgs,
            outputArguments = outputArgs
          )

          (0 until outputVectors.head.getValueCount).iterator.map { v_idx =>
            val writer = new UnsafeRowWriter(outputVectors.size)
            writer.reset()
            outputVectors.zipWithIndex.foreach { case (v, c_idx) =>
              if (v_idx < v.getValueCount) {
                v match {
                  case vector: Float8Vector =>
                    val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if (isNull) writer.setNullAt(c_idx)
                    else writer.write(c_idx, vector.get(v_idx))
                  case vector: IntVector =>
                    val isNull =
                      BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if (isNull) writer.setNullAt(c_idx)
                    else writer.write(c_idx, vector.get(v_idx))
                  case vector: BigIntVector =>
                    val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if (isNull) writer.setNullAt(c_idx)
                    else writer.write(c_idx, vector.get(v_idx))
                  case vector: SmallIntVector =>
                    val isNull = BitVectorHelper.get(vector.getValidityBuffer, v_idx) == 0
                    if (isNull) writer.setNullAt(c_idx)
                    else writer.write(c_idx, vector.get(v_idx))
                  case varChar: VarCharVector =>
                    val isNull = BitVectorHelper.get(varChar.getValidityBuffer, v_idx) == 0
                    if (isNull) writer.setNullAt(c_idx)
                    else writer.write(c_idx, varChar.get(v_idx))
                }
              }
            }
            writer.getRow
          }
        } finally {
          inputVectors.foreach(_.close())
        }
      }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    sortingMode match {
      case c @ Coalesced(cFunction) => coalesceAndExecute(c)
    }
  }
}