package com.nec.spark.planning.plans

import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}

case class VectorEngineJoinPlan(
  outputExpressions: Seq[NamedExpression],
  veFunction: VeFunction,
  left: SparkPlan,
  right: SparkPlan
) extends SparkPlan
  with BinaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanCallsVeFunction {

  override def executeVeColumnar(): RDD[VeColBatch] = ???

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(veFunction = f(veFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)
}
