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
package com.nec.spark

import com.nec.spark.LocalVeoExtension.compilerRule
import com.nec.spark.planning.{VERewriteStrategy, VeColumnarRule, VeRewriteStrategyOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

object LocalVeoExtension {
  var _enabled = true

  def compilerRule(sparkSession: SparkSession): ColumnarRule = new ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
      println(s"Received: ${plan}")
      plan
    }
  }
}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      new VERewriteStrategy(
        options = VeRewriteStrategyOptions.fromConfig(sparkSession.sparkContext.getConf)
      )
    )
    sparkSessionExtensions.injectColumnar(compilerRule)
    sparkSessionExtensions.injectColumnar(_ => new VeColumnarRule)
  }
}
