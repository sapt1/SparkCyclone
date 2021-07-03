package com.nec.testing

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

import java.nio.file.Path
import java.nio.file.Paths

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
        case SanityCheckSize => makeCsvNumsMultiColumn(sparkSession)
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

  final case class SampleRow(ColA: Double, ColB: Double)

  def makeMemoryNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    Seq[SampleRow](
      SampleRow(1, 2),
      SampleRow(2, -1),
      SampleRow(3, 1),
      SampleRow(4, -4),
      SampleRow(52, 11)
    )
      .toDS()
      .createOrReplaceTempView(SharedName)
  }

  val SampleColA = "ColA"
  val SampleColB = "ColB"

  def makeCsvNumsMultiColumn(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(
      Array(StructField(SampleColA, DoubleType), StructField(SampleColB, DoubleType))
    )

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleMultiColumnCSV.toString)
      .createOrReplaceTempView(SharedName)
  }

  lazy val LargeCSV: Path =
    Paths.get("/home/dominik/large-sample-csv-10_9/")

  def makeCsvNumsLarge(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(
      Array(
        StructField(SampleColA, DoubleType),
        StructField(SampleColB, DoubleType),
        StructField("c", DoubleType)
      )
    )

    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(LargeCSV.toString)
      .createOrReplaceTempView(SharedName)
  }

  def makeParquetNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .withColumnRenamed("a", SampleColA)
      .withColumnRenamed("b", SampleColB)
      .createOrReplaceTempView(SharedName)
  }

  lazy val LargeParquet: Path =
    Paths.get("/home/william/large-sample-parquet-10_9/")

  def makeParquetNumsLarge(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.read
      .format("parquet")
      .load(LargeParquet.toString)
      .withColumnRenamed("a", SampleColA)
      .withColumnRenamed("b", SampleColB)
      .createOrReplaceTempView(SharedName)
  }
}