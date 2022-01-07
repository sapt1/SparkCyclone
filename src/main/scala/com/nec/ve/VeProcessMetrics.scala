package com.nec.ve

trait VeProcessMetrics {
  def registerAllocation(amount: Long, position: Long): Unit
  def deregisterAllocation(position: Long): Unit
  def registerVeCall(timeTaken: Long): Unit
  def increaseSerializationTime(increaseBy: Long): Unit
  def increaseTransferTime(increaseBy: Long): Unit
}

object VeProcessMetrics {
  object NoOp extends VeProcessMetrics {
    override def registerAllocation(amount: Long, position: Long): Unit = ()
    override def deregisterAllocation(position: Long): Unit = ()
    override def increaseSerializationTime(increaseBy: Long): Unit = ()
    override def increaseTransferTime(increaseBy: Long): Unit = ()
    override def registerVeCall(timeTaken: Long): Unit = ()
  }
}
