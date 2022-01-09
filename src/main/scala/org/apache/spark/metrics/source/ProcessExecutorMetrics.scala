package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, Histogram, MetricRegistry, UniformReservoir}
import com.nec.ve.VeProcessMetrics
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class ProcessExecutorMetrics extends VeProcessMetrics with Source {
  private val allocations: scala.collection.mutable.Map[Long, Long] = mutable.Map.empty
  private val veCalls: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  private var totalTransferTime: Long = 0L
  private var arrowConversionTime: Long = 0L
  private val arrowConversionHist = new Histogram(new UniformReservoir())

  def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T = {
    val start = System.currentTimeMillis()
    val result = toMeasure
    val end = System.currentTimeMillis()

    registerTime(end - start)
    result
  }

  override def registerAllocation(amount: Long, position: Long): Unit =
    allocations.put(position, amount)

  override def registerConversionTime(timeTaken: Long): Unit = {
    arrowConversionTime += timeTaken
    arrowConversionHist.update(timeTaken)
  }

  override def registerTransferTime(timeTaken: Long): Unit = {
    totalTransferTime += timeTaken
  }

  override def deregisterAllocation(position: Long): Unit =
    allocations.remove(position)

  override def registerVeCall(timeTaken: Long): Unit = veCalls.append(timeTaken)

  override def sourceName: String = "VEProcessExecutor"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(
    MetricRegistry.name("ve", "allocations"),
    new Gauge[Long] {
      override def getValue: Long = allocations.size
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "arrowConversionTime"),
    new Gauge[Long] {
      override def getValue: Long = arrowConversionTime
    }
  )

  metricRegistry.register(MetricRegistry.name("ve", "arrowConversionTimeHist"), arrowConversionHist)

  metricRegistry.register(
    MetricRegistry.name("ve", "transferTime"),
    new Gauge[Long] {
      override def getValue: Long = totalTransferTime
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "callTime"),
    new Gauge[Long] {
      override def getValue: Long = veCalls.sum
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "calls"),
    new Gauge[Long] {
      override def getValue: Long = veCalls.size
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "bytesAllocated"),
    new Gauge[Long] {
      override def getValue: Long = allocations.valuesIterator.sum
    }
  )

  def getAllocations: Map[Long, Long] = allocations.toMap

}
