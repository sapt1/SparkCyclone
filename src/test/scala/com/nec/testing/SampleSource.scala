package com.nec.testing

import com.nec.spark.SampleTestData.LargeCSV
import com.nec.spark.SampleTestData.LargeParquet
import com.nec.spark.SampleTestData.SampleCSV
import com.nec.spark.SampleTestData.SampleMultiColumnCSV
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import org.apache.spark.sql.SparkSession
import com.nec.testing.Testing.DataSize.BenchmarkSize
import com.nec.testing.Testing.DataSize.SanityCheckSize
import com.nec.testing.Testing.DataSize
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

sealed trait SampleSource extends Serializable {
  def title: String
  def isColumnar: Boolean
  def generate(sparkSession: SparkSession, size: DataSize): Unit
}

object SampleSource {
  case object CSV extends SampleSource {
    override def isColumnar: Boolean = false
    override def generate(sparkSession: SparkSession, size: DataSize): Unit = {
      size match {
        case BenchmarkSize   => makeCsvNumsLarge(sparkSession)
        case SanityCheckSize => makeCsvNums(sparkSession)
      }
    }

    override def title: String = "CSV"
  }
  case object Parquet extends SampleSource {
    override def isColumnar: Boolean = true
    override def generate(sparkSession: SparkSession, size: DataSize): Unit = {
      size match {
        case BenchmarkSize   => makeParquetNumsLarge(sparkSession)
        case SanityCheckSize => makeParquetNums(sparkSession)
      }
    }

    override def title: String = "Parquet"
  }
  case object InMemory extends SampleSource {
    override def isColumnar: Boolean = true
    override def generate(sparkSession: SparkSession, size: DataSize): Unit =
      makeMemoryNums(sparkSession)
    override def title: String = "LocalTable"
  }

  val All: List[SampleSource] = List(CSV, Parquet, InMemory)


  val SharedName = "nums"

  def makeMemoryNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    Seq(1d, 2d, 3d, 4d, 52d)
      .toDS()
      .createOrReplaceTempView(SharedName)
  }

  def makeCsvNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType)))

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleCSV.toString)
      .withColumnRenamed("a", "value")
      .as[Double]
      .createOrReplaceTempView(SharedName)
  }

  def makeCsvNumsMultiColumn(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType), StructField("b", DoubleType)))

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .createOrReplaceTempView(SharedName)
  }

  def makeCsvNumsLarge(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType), StructField("b", DoubleType), StructField("c", DoubleType)))

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(LargeCSV.toString)
      .withColumnRenamed("a", "value")
      .createOrReplaceTempView(SharedName)
  }

  def makeParquetNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .withColumnRenamed("a", "value")
      .as[(Double, Double)]
      .createOrReplaceTempView(SharedName)
  }

  def makeParquetNumsLarge(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.read
      .format("parquet")
      .load(LargeParquet.toString)
      .withColumnRenamed("a", "value")
      .createOrReplaceTempView(SharedName)
  }
}
