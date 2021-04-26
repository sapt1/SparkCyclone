package com.nec.spark.agile

import java.nio.file.Paths
import com.nec.spark.{AcceptanceTest, Aurora4SparkDriver, Aurora4SparkExecutorPlugin, SqlPlugin}
import org.apache.log4j.{Level, Logger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

final class SqlPluginTest extends AnyFreeSpec with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    super.beforeAll()
  }

  "It is not launched if not specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    val sparkContext = new SparkContext(conf)

    try {
      assert(!Aurora4SparkDriver.launched, "Expect the driver to have not been launched")
      assert(
        !Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.isEmpty,
        "Expect the executor plugin to have not been launched"
      )
    } finally sparkContext.stop()
  }

  "It is launched if specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.plugins", classOf[SqlPlugin].getName)
    val sparkContext = new SparkContext(conf)
    try {
      assert(Aurora4SparkDriver.launched, "Expect the driver to have been launched")
      assert(
        Aurora4SparkExecutorPlugin.launched && !Aurora4SparkExecutorPlugin.params.isEmpty,
        "Expect the executor plugin to have been launched"
      )
    } finally sparkContext.stop()
  }

  "It properly passes aruments to spark executor plugin" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.plugins", classOf[SqlPlugin].getName)
    val sparkContext = new SparkContext(conf)
    try {
      assert(
        Aurora4SparkExecutorPlugin.params == Map("testArgument" -> "test"),
        "Expect arguments to be passed from driver to executor plugin"
      )
    } finally sparkContext.stop()
  }

  "We can run a Spark-SQL job" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
      assert(result == List(3))
    } finally sparkSession.close()
  }

  "We can get an execution plan for a sum of rows and dissect the programmatic structure" in {
    info("""
           |We do this so that we know exactly what we need to optimize/rewrite for GPU processing
           |
           |We will get there more quickly by doing this sort of use case based reverse engineering.
           |""".stripMargin)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq(1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      val result =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal].head()

      assert(
        SparkPlanSavingPlugin.savedSparkPlan.getClass.getCanonicalName ==
          "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
      )
      SparkPlanSavingPlugin.savedSparkPlan match {
        case first @ HashAggregateExec(
              requiredChildDistributionExpressions,
              groupingExpressions,
              aggregateExpressions,
              aggregateAttributes,
              initialInputBufferOffset,
              resultExpressions,
              child
            ) =>
          info(s"First root of the plan: ${first}")
          assert(
            child.getClass.getCanonicalName ==
              "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"
          )
          child match {
            case second @ org.apache.spark.sql.execution.exchange
                  .ShuffleExchangeExec(outputPartitioning, child2, shuffleOrigin) =>
              info(s"Second root of the plan: ${second}")
              assert(
                child2.getClass.getCanonicalName ==
                  "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
              )
              child2 match {
                case third @ org.apache.spark.sql.execution.aggregate
                      .HashAggregateExec(
                        requiredChildDistributionExpressions,
                        groupingExpressions,
                        aggregateExpressions,
                        aggregateAttributes,
                        initialInputBufferOffset,
                        resultExpressions,
                        child3
                      ) =>
                  info(s"Third root of the plan: ${third}")
                  assert(
                    child3.getClass.getCanonicalName ==
                      "org.apache.spark.sql.execution.LocalTableScanExec"
                  )
                  child3 match {
                    case fourth @ LocalTableScanExec(output, rows) =>
                      info(s"Fourth root of the plan: ${fourth}")
                      assert(output.length == 1, "There is only 1 column")
                      assert(rows.length == 3, "There are 3 rows")
                  }
              }
          }
      }

      assert(result == BigDecimal(6))
    } finally sparkSession.close()
  }

  "From the execution plan, we get the inputted numbers" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq(BigDecimal(1), BigDecimal(2), BigDecimal(3))
        .toDS()
        .createOrReplaceTempView("nums")

      val result =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal].head()

      assert(
        SparkPlanSavingPlugin.savedSparkPlan.getClass.getCanonicalName
          == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
      )

      assert(
        SumPlanExtractor
          .matchPlan(SparkPlanSavingPlugin.savedSparkPlan)
          .contains(List(BigDecimal(1), BigDecimal(2), BigDecimal(3)))
      )
    } finally sparkSession.close()
  }

  "We can do a pretend plug-in that will return 6" in {
    info("""
      This enables us to be confident this is the right place to do the rewriting.
      The significance of this is that from here,
      we will be able to compare expected results with actual results after we apply the plug-in
      """)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[DummyShortCircuitSqlPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      DummyShortCircuitSqlPlugin.applyShortCircuit = false
      List[BigDecimal](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")
      val sumDataSet =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]

      DummyShortCircuitSqlPlugin.applyShortCircuit = true
      val result = sumDataSet.head()

      assert(result == BigDecimal(6))
    } finally sparkSession.close()
  }

  "We call VE over SSH using the Python script, and get the right sum back from it" taggedAs
    AcceptanceTest in {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.set("spark.ui.enabled", "false")
      conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
      conf.setAppName("local-test")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        import sparkSession.implicits._
        SummingPlugin.enable = false

        val nums = List(
          BigDecimal(1),
          BigDecimal(2),
          BigDecimal(3),
          BigDecimal(4),
          BigDecimal(Math.abs(scala.util.Random.nextInt() % 200))
        )
        info(s"Input: ${nums}")

        nums
          .toDS()
          .createOrReplaceTempView("nums")

        SummingPlugin.enable = true
        SummingPlugin.summer = BigDecimalSummer.PythonNecSSHSummer

        val sumDataSet =
          sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]
        val result = sumDataSet.head()

        info(s"Result of sum = $result")
        assert(result == BigDecimalSummer.ScalaSummer.sum(nums))
      } finally sparkSession.close()
    }

  "We call VE over SSH using a Bundle, and get the right sum back from it" taggedAs
    AcceptanceTest in {
      markup("SUM() of single column")
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.set("spark.ui.enabled", "false")
      conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
      conf.setAppName("local-test")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        import sparkSession.implicits._
        SummingPlugin.enable = false

        val nums = List(
          BigDecimal(1),
          BigDecimal(2),
          BigDecimal(3),
          BigDecimal(4),
          BigDecimal(Math.abs(scala.util.Random.nextInt() % 200))
        )
        info(s"Input: ${nums}")

        nums
          .toDS()
          .createOrReplaceTempView("nums")

        SummingPlugin.enable = true
        SummingPlugin.summer = BigDecimalSummer.BundleNecSSHSummer

        val sumDataSet =
          sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]
        val result = sumDataSet.head()

        info(s"Result of sum = $result")
        assert(result == BigDecimalSummer.ScalaSummer.sum(nums))
      } finally sparkSession.close()
    }

  "We call the Scala summer, with a CSV input" in {
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      SummingPlugin.enable = false
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      SummingPlugin.enable = true

      val sumDataSet = sparkSession.read
        .format("csv")
        .schema(StructType(Seq(StructField("value", DecimalType.SYSTEM_DEFAULT, nullable = false))))
        .load(Paths.get(this.getClass.getResource("/sample.csv").toURI).toAbsolutePath.toString)
        .as[BigDecimal]
        .selectExpr("SUM(value)")
        .as[BigDecimal]

      sumDataSet.explain(true)
      val result = sumDataSet.head()

      info(s"Result of sum = $result")
      assert(result == BigDecimal(62))
    } finally sparkSession.close()
  }

  import SparkPlanSavingPlugin.savedSparkPlan

  "We match the averaging plan" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[BigDecimal](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT AVG(value) FROM nums").as[BigDecimal].head()
      info("\n" + savedSparkPlan.toString())
      assert(AveragingPlanner.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Summing plan does not match in the averaging plan" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[BigDecimal](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal].head()

      assert(AveragingPlanner.matchPlan(savedSparkPlan).isEmpty, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Summing plan does not match the averaging plan" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[BigDecimal](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT AVG(value) FROM nums").as[BigDecimal].head()

      assert(SumPlanExtractor.matchPlan(savedSparkPlan).isEmpty, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "We call VE with our Averaging plan" taggedAs
    AcceptanceTest in {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.set("spark.ui.enabled", "false")
      conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      conf.setAppName("local-test")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        import sparkSession.implicits._

        val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))
        info(s"Input: ${nums}")

        nums
          .toDS()
          .createOrReplaceTempView("nums")

        SparkSqlPlanExtension.rulesToApply.append { (sparkPlan) =>
          AveragingPlanner
            .matchPlan(sparkPlan)
            .map { childPlan =>
              AveragingSparkPlan(childPlan, AveragingSparkPlan.averageLocal)
            }
            .getOrElse(fail("Not expected to be here"))
        }

        val sumDataSet =
          sparkSession.sql("SELECT AVG(value) FROM nums").as[Double]

        val result = sumDataSet.head()

        assert(result == nums.sum / nums.length)
      } finally sparkSession.close()
    }

  "Spark's AVG() function returns a different Scale from SUM()" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    try {
      import sparkSession.implicits._

      val nums = List.empty[BigDecimal]

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SparkSqlPlanExtension.rulesToApply.append { _ => StaticNumberPlan(5) }

      val sumQuery = sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]
      val avgQuery = sparkSession.sql("SELECT AVG(value) FROM nums").as[BigDecimal]

      assert(
        sumQuery.head() == BigDecimal(5) && avgQuery.head() == BigDecimal(0.0005)
          && sumQuery.schema.head.dataType.asInstanceOf[DecimalType] == DecimalType(38, 18)
          && avgQuery.schema.head.dataType.asInstanceOf[DecimalType] == DecimalType(38, 22)
      )

    } finally sparkSession.close()
  }

}