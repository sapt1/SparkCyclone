package com.nec.spark.planning.simplesum
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.simplesum.SimpleSumPlan.SumMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.freespec.AnyFreeSpec
import com.eed3si9n.expecty.Expecty.assert
import com.nec.testing._
import com.nec.testing.Testing.TestingTarget
import com.nec.testing.SampleSource._
import org.apache.spark.sql.Dataset

object SimpleSumPlanTest {
  val PureJvmBasedModes: List[SumMethod] = List(
    SumMethod.NonCodegen.RDDBased,
    SumMethod.CodegenBased.ArrowCodegenBased(ArrowSummer.JVMBased),
    SumMethod.CodegenBased.JvmIncremental
  )

  def X86AvailableModes(libPath: String): List[SumMethod] = List(
    SumMethod.CodegenBased.ArrowCodegenBased(ArrowSummer.CBased(libPath))
  )

  def VhAvailableModes(libPath: String): List[SumMethod] = List(
    SumMethod.CodegenBased.ArrowCodegenBased(ArrowSummer.VeoBased)
  )

  final case class SimpleSumTesting(sumMethod: SumMethod, sampleSource: SampleSource, sql: String)
    extends Testing {
    type Result = Double

    override def verifyResult(data: List[Result]): Unit = {
      assert(data == List[Double](62))
    }

    override def prepareInput(
      sparkSession: SparkSession,
      dataSize: Testing.DataSize
    ): Dataset[Double] = {
      sampleSource.generate(sparkSession, dataSize)
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double]
    }

    override def prepareSession(): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(SimpleSumPlan(planLater(child), sumMethod))
                  case _ => Nil
                }
            }
          )
        )
        .getOrCreate()
    }
    override def testingTarget: Testing.TestingTarget = TestingTarget.PlainSpark
  }

  val OurTesting: List[Testing] = {
    for {
      sumMethod <- PureJvmBasedModes
      sampleSource <- SampleSource.All
      sql = s"SELECT SUM(${SampleColA}) FROM nums"
    } yield SimpleSumTesting(sumMethod, sampleSource, sql)
  }
}

final class SimpleSumPlanTest extends AnyFreeSpec with BenchTestAdditions {

  SimpleSumPlanTest.OurTesting.foreach(runTestCase)

}