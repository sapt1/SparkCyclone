package com.nec.spark.planning

import scala.collection.JavaConverters.asScalaBufferConverter

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.ve.VeColBatch
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, DualMode}
import org.apache.spark.{SparkContext, TaskContext}
import SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerConversionTime}

object VeColBatchConverters {

  def getNumRows(sparkContext: SparkContext, conf: SQLConf): Int = {
    sparkContext.getConf
      .getOption("com.nec.spark.ve.columnBatchSize")
      .map(_.toInt)
      .getOrElse(conf.columnBatchSize)
  }

  object BasedOnColumnarBatch {
    object ColB {
      def unapply(internalRow: InternalRow): Option[VeColBatch] = ???
    }
  }

  final case class UnInternalVeColBatch(veColBatch: VeColBatch)

  def internalRowToVeColBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[UnInternalVeColBatch] = {
    input.mapPartitions { iterator =>
      DualMode.handleIterator(iterator) match {
        case Left(colBatches) =>
          colBatches.map(v => UnInternalVeColBatch(veColBatch = v))
        case Right(rowIterator) =>
          if (rowIterator.hasNext) {
            lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
            new Iterator[UnInternalVeColBatch] {
              private val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
              private val root = VectorSchemaRoot.create(arrowSchema, allocator)
              private val cb = new ColumnarBatch(
                root.getFieldVectors.asScala
                  .map(vector => new ArrowColumnVector(vector))
                  .toArray,
                root.getRowCount
              )

              private val arrowWriter = ArrowWriter.create(root)
              arrowWriter.finish()
              TaskContext.get().addTaskCompletionListener[Unit] { _ =>
                cb.close()
              }

              override def hasNext: Boolean = {
                rowIterator.hasNext
              }

              override def next(): UnInternalVeColBatch = {
                measureRunningTime {
                  arrowWriter.reset()
                  cb.setNumRows(0)
                  root.getFieldVectors.asScala.foreach(_.reset())
                  var rowCount = 0
                  while (rowCount < numRows && rowIterator.hasNext) {
                    val row = rowIterator.next()
                    arrowWriter.write(row)
                    arrowWriter.finish()
                    rowCount += 1
                  }
                  cb.setNumRows(rowCount)
                }(registerConversionTime)

                import SparkCycloneExecutorPlugin.veProcess
                val newBatch =
                  try UnInternalVeColBatch(veColBatch = VeColBatch.fromArrowColumnarBatch(cb))
                  finally cb.close()
                SparkCycloneExecutorPlugin.register(newBatch.veColBatch)
                newBatch
              }
            }
          } else {
            Iterator.empty
          }
      }
    }

  }

}
