package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types.{Decimal, DecimalType}

/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object SumPlanExtractor {
  def matchPlan(sparkPlan: SparkPlan): Option[List[BigDecimal]] = {
    matchSumChildPlan(sparkPlan).collectFirst {
      case LocalTableScanExec(
            Seq(AttributeReference(name, dataType: DecimalType, nullable, metadata)),
            rows
          ) =>
        dataType match {
          case DecimalType(precision, scale) =>
            rows
              .map(_.get(0, dataType).asInstanceOf[Decimal])
              .map(_.toBigDecimal)
              .toList
        }
    }
  }

  def matchSumChildPlan(sparkPlan: SparkPlan): Option[SparkPlan] = {
    PartialFunction.condOpt(sparkPlan) {
      case first @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(Sum(_), _, _, _, _)),
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            org.apache.spark.sql.execution.exchange
              .ShuffleExchangeExec(
                outputPartitioning,
                org.apache.spark.sql.execution.aggregate
                  .HashAggregateExec(
                    _requiredChildDistributionExpressions,
                    _groupingExpressions,
                    _aggregateExpressions,
                    _aggregateAttributes,
                    _initialInputBufferOffset,
                    _resultExpressions,
                    fourth
                  ),
                shuffleOrigin
              )
          ) =>
        fourth
    }
  }

}