/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.cgescape

import com.nec.spark.cgescape.IdentityCodegenBatchPlan.UnsafeExternalDuplicator
import com.nec.spark.cgescape.UnsafeExternalProcessorBase.UnsafeBatchProcessor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.BlockingOperatorWithCodegen
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan

/**
 * This is a simple batch-based plan, which simply returns the results that were requested
 */
final case class IdentityCodegenBatchPlan(child: SparkPlan)
  extends SparkPlan
  with BlockingOperatorWithCodegen
  with UnsafeExternalProcessorBase {
  override type ContainerType = UnsafeExternalDuplicator
  override def containerClass: Class[ContainerType] = classOf[UnsafeExternalDuplicator]
  override def createContainer(): UnsafeExternalDuplicator = new UnsafeExternalDuplicator
  override def children: Seq[SparkPlan] = Seq(child)
  protected def inputAttributes: Seq[Attribute] = child.output
  override def output: Seq[Attribute] = child.output
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
}

object IdentityCodegenBatchPlan {

  /** Collect UnsafeRows, and then emit UnsafeRows */
  final class UnsafeExternalDuplicator extends UnsafeBatchProcessor {
    private val rows = scala.collection.mutable.Buffer.empty[UnsafeRow]
    def insertRow(unsafeRow: UnsafeRow): Unit = rows.append(unsafeRow.copy())
    def execute(): Iterator[InternalRow] = rows.iterator
  }
}
