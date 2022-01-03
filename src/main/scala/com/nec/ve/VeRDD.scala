package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.ve.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

object VeRDD extends LazyLogging {
  def exchange(rdd: RDD[(Int, VeColVector)])(implicit veProcess: VeProcess): RDD[VeColVector] =
    rdd
      .mapPartitions(
        f = iter =>
          iter.map { case (p, v) =>
            (p, (v, v.serialize()))
          },
        preservesPartitioning = true
      )
      .repartitionByKey()
      .mapPartitions(
        f = iter => iter.map { case (_, (v, ba)) => v.deserialize(ba) },
        preservesPartitioning = true
      )

  def exchangeL(rdd: RDD[(Int, List[VeColVector])]): RDD[List[VeColVector]] =
    rdd
      .mapPartitions(
        f = iter =>
          iter.map { case (p, v) =>
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            logger.debug(s"Preparing to serialize batch ${v}")
            val r = (p, (v, v.map(_.serialize())))
            logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
            r
          },
        preservesPartitioning = true
      )
      .repartitionByKey()
      .mapPartitions(
        f = iter =>
          iter.map { case (_, (v, ba)) =>
            v.zip(ba).map { case (vv, bb) =>
              logger.debug(s"Preparing to deserialize batch ${vv}")
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              val res = vv.deserialize(bb)
              logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
              res
            }
          },
        preservesPartitioning = true
      )

  def joinExchangeLB(
    left: RDD[(Int, VeColBatch)],
    right: RDD[(Int, VeColBatch)]
  ): RDD[(List[VeColVector], List[VeColVector])] = {
    joinExchangeL(left.map { case (k, b) => k -> b.cols }, right.map { case (k, b) => k -> b.cols })
  }

  def joinExchangeL(
    left: RDD[(Int, List[VeColVector])],
    right: RDD[(Int, List[VeColVector])]
  ): RDD[(List[VeColVector], List[VeColVector])] = {
    {
      val leftPts = left
        .mapPartitions(
          f = iter =>
            iter.map { case (p, v) =>
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              logger.debug(s"Preparing to serialize batch ${v}")
              val r = (p, (v, v.map(_.serialize())))
              logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
              r
            },
          preservesPartitioning = true
        )
      val rightPts = right
        .mapPartitions(
          f = iter =>
            iter.map { case (p, v) =>
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              logger.debug(s"Preparing to serialize batch ${v}")
              val r = (p, (v, v.map(_.serialize())))
              logger.debug(s"Completed serializing batch ${v} (${r._2._2.map(_.length)} bytes)")
              r
            },
          preservesPartitioning = true
        )

      leftPts.join(rightPts).repartitionByKey().map { case (_, ((v1, ba1), (v2, ba2))) =>
        val first = v1.zip(ba1).map { case (vv, bb) =>
          logger.debug(s"Preparing to deserialize batch ${vv}")
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          val res = vv.deserialize(bb)
          logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
          res
        }
        val second = v2.zip(ba2).map { case (vv, bb) =>
          logger.debug(s"Preparing to deserialize batch ${vv}")
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          val res = vv.deserialize(bb)
          logger.debug(s"Completed deserializing batch ${vv} --> ${res}")
          res
        }

        (first, second)
      }
    }
  }

  def exchangeLS(rdd: RDD[(Int, List[VeColVector])])(implicit
    veProcess: VeProcess
  ): RDD[List[VeColVector]] =
    rdd.repartitionByKey().mapPartitions(f = _.map(_._2), preservesPartitioning = true)

  implicit class RichKeyedRDD(rdd: RDD[(Int, VeColVector)]) {
    def exchangeBetweenVEs()(implicit veProcess: VeProcess): RDD[VeColVector] = exchange(rdd)
  }

  implicit class IntKeyedRDD[V: ClassTag](rdd: RDD[(Int, V)]) {
    def repartitionByKey(): RDD[(Int, V)] =
      new ShuffledRDD[Int, V, V](rdd, new HashPartitioner(rdd.partitions.length))
  }
  implicit class RichKeyedRDDL(rdd: RDD[(Int, List[VeColVector])]) {
    def exchangeBetweenVEs(): RDD[List[VeColVector]] = exchangeL(rdd)
    // for single-machine case!
    // def exchangeBetweenVEsNoSer()(implicit veProcess: VeProcess): RDD[List[VeColVector]] =
    // exchangeLS(rdd)
  }
}
