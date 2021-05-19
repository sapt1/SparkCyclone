package com.nec.spark.agile

import com.nec.spark.agile.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GenericAggregationPlanOffHeap(child: SparkPlan,
                                         outputColumns: Seq[OutputColumn]
                                        ) extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child
      .executeColumnar()
      .map { columnarBatch =>
        val offHeapAggregations = outputColumns.map {
          case OutputColumn(inputColumns, outputColumnIndex, columnAggregation, outputAggregator) => {
            val dataVectors = inputColumns
              .map(column => columnarBatch.column(column.index).asInstanceOf[OffHeapColumnVector])
              .map(vector =>
                outputAggregator.aggregateOffHeap(vector.valuesNativeAddress(),
                  columnarBatch.numRows())
              )

            DataColumnAggregation(outputColumnIndex,
              columnAggregation,
              dataVectors,
              columnarBatch.numRows()
            )
          }
        }

        offHeapAggregations
      }
      .coalesce(1)
      .mapPartitions(its => {

        val aggregated = its.toList.flatten.groupBy(_.outputColumnIndex).map {
          case (idx, columnAggregations) =>
            columnAggregations.reduce((a, b) => a.combine(b)(_ + _))
        }

        val elementsSum = aggregated.toList.sortBy(_.outputColumnIndex).map {
          case DataColumnAggregation(outIndex, NoAggregation, columns, _) => columns.head
          case DataColumnAggregation(outIndex, Addition, columns, _) => columns.sum
          case DataColumnAggregation(outIndex, Subtraction, columns, _) =>
            columns.reduce((a, b) => a - b)
        }

        val vectors = elementsSum.map(_ => new OnHeapColumnVector(1, DoubleType))

        elementsSum.zip(vectors).foreach { case (sum, vector) =>
          vector.putDouble(0, sum)
        }

        Iterator(new ColumnarBatch(vectors.toArray, 1))
      })
  }

  override def output: Seq[Attribute] = outputColumns.map {
    case OutputColumn(inputColumns, outputColumnIndex, columnAggregation, outputAggregator) =>
      AttributeReference(name = "_" + outputColumnIndex, dataType = DoubleType, nullable = false)()
  }

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def doExecute(): RDD[InternalRow] = sys.error("Row production not supported")
}