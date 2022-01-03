package com.nec.spark.planning.plans

import com.nec.spark.planning.{
  PlanCallsVeFunction,
  SupportsKeyedVeColBatch,
  SupportsVeColBatch,
  VeFunction
}
import com.nec.ve.{VeColBatch, VeRDD}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}

case class VectorEngineJoinPlan(
  outputExpressions: Seq[NamedExpression],
  joinFunction: VeFunction,
  left: SparkPlan,
  right: SparkPlan
) extends SparkPlan
  with BinaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanCallsVeFunction {

  override def executeVeColumnar(): RDD[VeColBatch] =
    VeRDD
      .joinExchangeLB(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed(),
        right = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed()
      )
      .mapPartitions { iterL =>
        logger.info(s"Will map multiple col batches for hash exchange.")
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        import com.nec.spark.SparkCycloneExecutorPlugin.source
        withVeLibrary { libRefJoin =>
          iterL.map { case (leftListVcv, rightListVcv) =>
            val leftColBatch = VeColBatch.fromList(leftListVcv)
            val rightColBatch = VeColBatch.fromList(rightListVcv)
            logger.debug(s"Mapping ${leftColBatch} / ${rightColBatch} for join")
            val batch =
              try veProcess.execute(
                libraryReference = libRefJoin,
                functionName = joinFunction.functionName,
                cols = leftColBatch.cols ++ rightColBatch.cols,
                results = joinFunction.results
              )
              finally {
                dataCleanup.cleanup(leftColBatch)
                dataCleanup.cleanup(rightColBatch)
              }
            logger.debug(s"Completed ${leftColBatch} / ${rightColBatch} => ${batch}.")
            VeColBatch.fromList(batch)
          }
        }
      }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
