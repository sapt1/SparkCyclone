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
package com.nec.spark.agile.groupby

import com.nec.cmake.TcpDebug
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CFunction, CVector, TypedCExpression2}
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.StringProducer
import com.nec.spark.agile.StringProducer.FilteringProducer
import com.nec.spark.agile.groupby.GroupByOutline.{
  storeTo,
  GroupingKey,
  StagedAggregation,
  StagedProjection,
  StringReference
}

final case class GroupByPartialGenerator(
  finalGenerator: GroupByPartialToFinalGenerator,
  computedGroupingKeys: List[(GroupingKey, Either[StringReference, TypedCExpression2])],
  computedProjections: List[(StagedProjection, Either[StringReference, TypedCExpression2])],
  stringVectorComputations: List[StringHoleEvaluation]
) {
  import finalGenerator._
  import stagedGroupBy._

  def createFull(inputs: List[CVector]): CFunction = {
    val finalFunction = finalGenerator.createFinal
    val partialFunction = createPartial(inputs)
    CFunction(
      inputs = partialFunction.inputs,
      outputs = finalFunction.outputs,
      body = CodeLines.from(
        CodeLines.commentHere(
          "Declare the variables for the output of the Partial stage for the unified function"
        ),
        partialFunction.outputs.map(cv => GroupByOutline.declare(cv)),
        partialFunction.body.blockCommented("Perform the Partial computation stage"),
        finalFunction.body.blockCommented("Perform the Final computation stage"),
        partialFunction.outputs
          .map(cv => GroupByOutline.dealloc(cv))
          .blockCommented("Deallocate the partial variables")
      )
    )
  }

  def createPartial(inputs: List[CVector]): CFunction =
    CFunction(
      inputs = inputs,
      outputs = partialOutputs,
      body = CodeLines.from(
        TcpDebug.conditional.createSock,
        CodeLines
          .from(
            performGrouping(count = s"${inputs.head.name}->count")
              .time("Grouping"),
            stringVectorComputations.map(_.computeVector).time("Compute String vectors"),
            computeGroupingKeysPerGroup.block.time("Compute grouping keys per group"),
            computedProjections.map { case (sp, e) =>
              computeProjectionsPerGroup(sp, e).time(s"Compute projection ${sp.name}")
            },
            computedAggregates.map { case (a, ag) =>
              computeAggregatePartialsPerGroup(a, ag).time(s"Compute aggregate ${a.name}")
            },
            stringVectorComputations.map(_.deallocData).time("Compute String vectors")
          )
          .time("Execution of Partial"),
        TcpDebug.conditional.close
      )
    )

  def computeProjectionsPerGroup(
    stagedProjection: StagedProjection,
    r: Either[StringReference, TypedCExpression2]
  ): CodeLines = r match {
    case Left(StringReference(sourceName)) =>
      val fp =
        FilteringProducer(
          s"partial_str_${stagedProjection.name}",
          StringProducer.copyString(sourceName)
        )
      CodeLines.from(
        CodeLines.debugHere,
        fp.setup,
        groupingCodeGenerator.forHeadOfEachGroup(CodeLines.from(fp.forEach)),
        fp.complete,
        groupingCodeGenerator.forHeadOfEachGroup(CodeLines.from(fp.validityForEach("g")))
      )
    case Right(TypedCExpression2(veType, cExpression)) =>
      CodeLines.from(
        CodeLines.debugHere,
        GroupByOutline
          .initializeScalarVector(
            veType,
            s"partial_${stagedProjection.name}",
            groupingCodeGenerator.groupsCountOutName
          ),
        groupingCodeGenerator.forHeadOfEachGroup(
          GroupByOutline.storeTo(s"partial_${stagedProjection.name}", cExpression, "g")
        )
      )
  }

  def computeAggregatePartialsPerGroup(
    stagedAggregation: StagedAggregation,
    aggregate: Aggregation
  ): CodeLines = {
    val prefix = s"partial_${stagedAggregation.name}"
    CodeLines.from(
      CodeLines.debugHere,
      stagedAggregation.attributes.map(attribute =>
        GroupByOutline.initializeScalarVector(
          veScalarType = attribute.veScalarType,
          variableName = s"partial_${attribute.name}",
          countExpression = groupingCodeGenerator.groupsCountOutName
        )
      ),
      CodeLines.debugHere,
      groupingCodeGenerator.forEachGroupItem(
        beforeFirst = aggregate.initial(prefix),
        perItem = aggregate.iterate(prefix),
        afterLast =
          CodeLines.from(stagedAggregation.attributes.zip(aggregate.partialValues(prefix)).map {
            case (attr, (_, ex)) =>
              CodeLines.from(GroupByOutline.storeTo(s"partial_${attr.name}", ex, "g"))
          })
      )
    )
  }

  def performGrouping(count: String): CodeLines =
    CodeLines.debugHere ++ groupingCodeGenerator.identifyGroups(
      tupleTypes = tupleTypes,
      tupleType = tupleType,
      count = count,
      thingsToGroup = computedGroupingKeys.map { case (_, e) =>
        e.map(_.cExpression).left.map(_.name)
      }
    )

  def computeGroupingKeysPerGroup: CodeLines = {
    final case class ProductionTriplet(init: CodeLines, forEach: CodeLines, complete: CodeLines)
    val initVars = computedGroupingKeys.map {
      case (groupingKey, Right(TypedCExpression2(scalarType, cExp))) =>
        ProductionTriplet(
          init = GroupByOutline.initializeScalarVector(
            veScalarType = scalarType,
            variableName = s"partial_${groupingKey.name}",
            countExpression = groupingCodeGenerator.groupsCountOutName
          ),
          forEach = storeTo(s"partial_${groupingKey.name}", cExp, "g"),
          complete = CodeLines.empty
        )
      case (groupingKey, Left(StringReference(sr))) =>
        val fp =
          FilteringProducer(s"partial_str_${groupingKey.name}", StringProducer.copyString(sr))

        ProductionTriplet(
          init = fp.setup,
          forEach = fp.forEach,
          complete = CodeLines.from(
            fp.complete,
            groupingCodeGenerator.forHeadOfEachGroup(fp.validityForEach("g"))
          )
        )
    }

    CodeLines.from(
      CodeLines.debugHere,
      initVars.map(_.init),
      CodeLines.debugHere,
      groupingCodeGenerator.forHeadOfEachGroup(initVars.map(_.forEach)),
      CodeLines.debugHere,
      initVars.map(_.complete)
    )
  }

}
