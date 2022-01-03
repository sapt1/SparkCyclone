package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.spark.planning.VeColBatchConverters.internalRowToVeColBatch
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.DetectVectorEngineSpec.VeClusterConfig
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.ve.RDDSpec.{
  doubleBatches,
  exchangeBatches,
  longBatches,
  testJoin,
  MultiFunctionName,
  RichDoubleList,
  RichVeColVector
}
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import com.nec.ve.VeProcess.{DeferredVeProcess, WrappingVeo}
import com.nec.ve.VeRDD.RichKeyedRDD
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BigIntVector, Float8Vector, IntVector}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalatest.freespec.AnyFreeSpec

import scala.collection.JavaConverters.asScalaIteratorConverter

final class RDDSpec extends AnyFreeSpec with SparkAdditions with VeKernelInfra {

  "A dataset from ColumnarBatches can be read via the carrier columnar vector" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>
    implicit val source: VeColVectorSource = VeColVectorSource(
      s"Process ${SparkCycloneExecutorPlugin._veo_proc}, executor ${SparkEnv.get.executorId}"
    )

    implicit val veProc: VeProcess =
      DeferredVeProcess(() =>
        WrappingVeo(SparkCycloneExecutorPlugin._veo_proc, source, VeProcessMetrics.NoOp)
      )
    def makeColumnarBatch1() = {
      val vec1 = {
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        new IntVector("test", rootAllocator)
      }
      vec1.setValueCount(5)
      vec1.setSafe(0, 10)
      vec1.setSafe(1, 20)
      vec1.setSafe(2, 30)
      vec1.setSafe(3, 40)
      vec1.setSafe(4, 50)
      new ColumnarBatch(Array(new ArrowColumnVector(vec1)), 5)
    }

    def makeColumnarBatch2() = {
      val vec2 = {
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        new IntVector("test", rootAllocator)
      }
      vec2.setValueCount(4)
      vec2.setSafe(0, 60)
      vec2.setSafe(1, 70)
      vec2.setSafe(2, 80)
      vec2.setSafe(3, 90)
      new ColumnarBatch(Array(new ArrowColumnVector(vec2)), 4)
    }
    val result = internalRowToVeColBatch(
      input = sparkSession.sparkContext
        .makeRDD(Seq(1, 2))
        .repartition(1)
        .map(int =>
          VeColBatch.fromArrowColumnarBatch(
            if (int == 1) makeColumnarBatch1()
            else makeColumnarBatch2()
          )
        )
        .mapPartitions(it => it.flatMap(_.toInternalColumnarBatch().rowIterator().asScala)),
      timeZoneId = "UTC",
      schema = StructType(Array(StructField("test", IntegerType))),
      numRows = 100
    ).mapPartitions(vcbi => {
      implicit val rootAllocator: RootAllocator = new RootAllocator()
      vcbi
        .map(vcb => {
          println(vcb)
          vcb.veColBatch
        })
        .map(_.toArrowColumnarBatch())
        .map(cb => cb.column(0).getArrowValueVector)
        .flatMap(fv => (0 until fv.getValueCount).map(idx => fv.asInstanceOf[IntVector].get(idx)))
    }).collect()
      .toList
      .sorted

    assert(result == List(10, 20, 30, 40, 50, 60, 70, 80, 90))
  }
  "We can pass around some Arrow things" in withSparkSession2(identity) { sparkSession =>
    longBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 500, step = 1, numSlices = 4)
    }
      .foreach(println)
  }

  "We can perform a VE call on Arrow things" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>
    implicit val source: VeColVectorSource = VeColVectorSource(
      s"Process ${SparkCycloneExecutorPlugin._veo_proc}, executor ${SparkEnv.get.executorId}"
    )

    implicit val veProc: VeProcess =
      DeferredVeProcess(() =>
        WrappingVeo(SparkCycloneExecutorPlugin._veo_proc, source, VeProcessMetrics.NoOp)
      )
    val result = compiledWithHeaders(DoublingFunction.toCodeLinesNoHeaderOutPtr("f").cCode) {
      path =>
        val ref = veProc.loadLibrary(path)
        doubleBatches {
          sparkSession.sparkContext
            .range(start = 1, end = 500, step = 1, numSlices = 4)
            .map(_.toDouble)
        }.map(arrowVec => VeColVector.fromFloat8Vector(arrowVec))
          .map(ve => veProc.execute(ref, "f", List(ve), DoublingFunction.outputs.map(_.veType)))
          .map(vectors => {
            WithTestAllocator { implicit alloc =>
              val vec = vectors.head.toArrowVector().asInstanceOf[Float8Vector]
              try vec.toList
              finally vec.close()
            }
          })
          .collect()
          .toList
          .flatten
          .sorted
    }

    val expected = List.range(1, 500).map(_.toDouble).map(_ * 2)
    expect(result == expected)
  }

  "Exchange data across partitions in cluster mode" in withSparkSession2(
    VeClusterConfig
      .andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
  ) { sparkSession =>
    val result =
      compiledWithHeaders(PartitioningFunction.toCodeLinesNoHeaderOutPtr(MultiFunctionName).cCode) {
        path =>
          val pathStr = path.toString
          exchangeBatches(sparkSession, pathStr)
            .collect()
            .toList
            .toSet
      }

    val expected = List[Double](199, 299, 399, 500).toSet
    expect(result == expected)
  }

  "Join data across partitioned data" in {
    val result =
      withSparkSession2(
        VeClusterConfig
          .andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
      ) { sparkSession =>
        testJoin(sparkSession)
      }.sortBy(_._1.head)

    val expected: List[(List[Double], List[Double])] =
      List(
        List[Double](3, 4, 5) -> List[Double](5, 6, 7),
        List[Double](5, 6, 7) -> List[Double](8, 8, 7)
      )

    assert(result == expected)
  }

}

object RDDSpec {

  def testJoin(sparkSession: SparkSession): List[(List[Double], List[Double])] = {

    val partsL: List[(Int, List[Double])] =
      List(1 -> List(3, 4, 5), 2 -> List(5, 6, 7))
    val partsR: List[(Int, List[Double])] =
      List(1 -> List(5, 6, 7), 2 -> List(8, 8, 7), 3 -> List(9, 6, 7))
    import SparkCycloneExecutorPlugin._
    VeRDD
      .joinExchangeLB(
        sparkSession.sparkContext.makeRDD(partsL).map { case (i, l) =>
          i -> VeColBatch.fromList(List(l.toVeColVector()))
        },
        sparkSession.sparkContext.makeRDD(partsR).map { case (i, l) =>
          i -> VeColBatch.fromList(List(l.toVeColVector()))
        }
      )
      .map { case (la, lb) =>
        (la.flatMap(_.toList[Double]), lb.flatMap(_.toList[Double]))
      }
      .collect()
      .toList
  }

  implicit class RichDoubleList(l: List[Double]) {
    def toVeColVector()(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      WithTestAllocator { implicit alloc =>
        withDirectFloat8Vector(l) { vec =>
          VeColVector.fromFloat8Vector(vec)
        }
      }
    }
  }

  trait ColVectorDecoder[T] {
    def decode(
      veColVector: VeColVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): List[T]
  }
  object ColVectorDecoder {
    implicit val decodeDouble: ColVectorDecoder[Double] = new ColVectorDecoder[Double] {
      override def decode(
        veColVector: VeColVector
      )(implicit veProcess: VeProcess, source: VeColVectorSource): List[Double] = {
        WithTestAllocator { implicit alloc =>
          veColVector.toArrowVector().asInstanceOf[Float8Vector].toList
        }
      }
    }
  }
  implicit class RichVeColVector(veColVector: VeColVector) {
    def toList[T: ColVectorDecoder](implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): List[T] =
      implicitly[ColVectorDecoder[T]].decode(veColVector)
  }

  val MultiFunctionName = "f_multi"

  private def exchangeBatches(sparkSession: SparkSession, pathStr: String): RDD[Double] = {

    import SparkCycloneExecutorPlugin.{veProcess => veProc}

    doubleBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 501, step = 1, numSlices = 4)
        .map(_.toDouble)
    }
      .mapPartitions(
        f = veIterator =>
          veIterator
            .map(arrowVec => {
              import SparkCycloneExecutorPlugin.source
              try VeColVector.fromFloat8Vector(arrowVec)
              finally arrowVec.close()
            })
            .flatMap(veColVector => {
              import SparkCycloneExecutorPlugin.source
              try {
                val ref = veProc.loadLibrary(java.nio.file.Paths.get(pathStr))

                veProc
                  .executeMulti(
                    ref,
                    MultiFunctionName,
                    List(veColVector),
                    List(CFunctionGeneration.VeScalarType.veNullableDouble)
                  )
                  .map { case (k, vs) => (k, vs.head) }
              } finally veColVector.free()
            }),
        preservesPartitioning = true
      )
      .exchangeBetweenVEs()
      .mapPartitions(vectorIter =>
        Iterator
          .continually {
            vectorIter.flatMap { vector =>
              WithTestAllocator { implicit alloc =>
                val vec = vector.toArrowVector().asInstanceOf[Float8Vector]
                val vl = vec.toList
                try if (vl.isEmpty) None else Some(vl.max)
                finally vec.close()
              }
            }.max
          }
          .take(1)
      )
  }

  final case class NativeFunction(name: String)

  def longBatches(rdd: RDD[Long]): RDD[BigIntVector] = {
    rdd.mapPartitions(iteratorLong =>
      Iterator
        .continually {
          val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"allocator for longs", 0, Long.MaxValue)
          val theList = iteratorLong.toList
          val vec = new BigIntVector("input", allocator)
          theList.iterator.zipWithIndex.foreach { case (v, i) =>
            vec.setSafe(i, v)
          }
          vec.setValueCount(theList.size)
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            vec.close()
            allocator.close()
          }
          vec
        }
        .take(1)
    )
  }
  def doubleBatches(rdd: RDD[Double]): RDD[Float8Vector] = {
    rdd.mapPartitions(iteratorDouble =>
      Iterator
        .continually {
          val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"allocator for floats", 0, Long.MaxValue)
          val theList = iteratorDouble.toList
          val vec = new Float8Vector("input", allocator)
          theList.iterator.zipWithIndex.foreach { case (v, i) =>
            vec.setSafe(i, v)
          }
          vec.setValueCount(theList.size)
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            vec.close()
            allocator.close()
          }
          vec
        }
        .take(1)
    )
  }

}
