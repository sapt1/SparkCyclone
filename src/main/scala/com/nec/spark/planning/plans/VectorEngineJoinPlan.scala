package com.nec.spark.planning.plans

import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
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
    left
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .zipPartitions(right.asInstanceOf[SupportsVeColBatch].executeVeColumnar()) {
        case (leftCB, rightCB) =>
          logger.info(s"Will map multiple col batches for hash exchange.")
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          import com.nec.spark.SparkCycloneExecutorPlugin.source
          withVeLibrary { libRefJoin =>
            val leftColumnBatches = leftCB.toList

            TaskContext.get().addTaskCompletionListener[Unit] { _ =>
              Option(leftColumnBatches).toList.flatten
                .foreach(leftColBatch =>
                  left.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(leftColBatch)
                )
            }

            rightCB.flatMap { rightColBatch =>
              try {
                leftColumnBatches.flatMap { leftColBatch =>
                  logger.debug(s"Mapping ${leftColBatch} / ${rightColBatch} for join")
                  val batch = veProcess.execute(
                    libraryReference = libRefJoin,
                    functionName = joinFunction.functionName,
                    cols = leftColBatch.cols ++ rightColBatch.cols,
                    results = joinFunction.results
                  )
                  logger.debug(s"Completed ${leftColBatch} / ${rightColBatch} => ${batch}.")
                  Option(VeColBatch.fromList(batch)).filter(_.nonEmpty).toList
                }
              } finally {
                right.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(rightColBatch)
              }
            }

          }
      }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
