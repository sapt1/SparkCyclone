package com.nec.spark.agile

import com.nec.spark.agile.AveragingSparkPlanOffHeap.OffHeapDoubleAverager
import com.nec.spark.agile.SingleValueStubPlan.SparkDefaultColumnName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch
import sun.misc.Unsafe

object AveragingSparkPlanOffHeap {

  trait OffHeapDoubleAverager extends Serializable {
    def average(memoryLocation: Long, count: Int): Double
  }

  object OffHeapDoubleAverager {
    object UnsafeBased extends OffHeapDoubleAverager {
      private def getUnsafe: Unsafe = {
        val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
        theUnsafe.setAccessible(true)
        theUnsafe.get(null).asInstanceOf[Unsafe]
      }
      override def average(memoryLocation: Long, count: ColumnIndex): Double = {
        (0 until count)
          .map { i =>
            getUnsafe.getDouble(memoryLocation + i * 8)
          }
          .toList
          .sum / count
      }
    }
    object VeoBased extends OffHeapDoubleAverager {
      override def average(memoryLocation: Long, count: ColumnIndex): Double = {
        (0 until count)
          .map { i =>
            getUnsafe.getDouble(memoryLocation + i * 8)
          }
          .toList
          .sum / count
      }
    }
  }

}

case class AveragingSparkPlanOffHeap(child: RowToColumnarExec, averager: OffHeapDoubleAverager)
  extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .doExecuteColumnar()
      .map { columnarBatch =>
        val theCol = columnarBatch.column(0).asInstanceOf[OffHeapColumnVector]
        (
          averager.average(theCol.valuesNativeAddress(), columnarBatch.numRows()),
          columnarBatch.numRows()
        )
      }
      .coalesce(1)
      .mapPartitions { nums =>
        Iterator {
          val nl = nums.toList
          val totalSize = nl.map(_._2).sum
          nl.map { case (avgs, gs) => avgs * (gs.toDouble / totalSize) }.sum
        }
      }
      .map { double =>
        val vector = new OnHeapColumnVector(1, DoubleType)
        vector.putDouble(0, double)
        new ColumnarBatch(Array(vector), 1)
      }
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}
