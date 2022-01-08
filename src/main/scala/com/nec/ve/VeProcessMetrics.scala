package com.nec.ve

trait VeProcessMetrics {
  def registerAllocation(amount: Long, position: Long): Unit
  def deregisterAllocation(position: Long): Unit
  def registerVeCall(timeTaken: Long): Unit
  def registerConversionTime(timeTaken: Long): Unit
  def registerTransferTime(timeTaken: Long): Unit

}

object VeProcessMetrics {
  object NoOp extends VeProcessMetrics {
    override def registerAllocation(amount: Long, position: Long): Unit = ()
    override def deregisterAllocation(position: Long): Unit = ()
    override def registerConversionTime(timeTaken: Long): Unit = ()
    override def registerTransferTime(timeTaken: Long): Unit = ()
    override def registerVeCall(timeTaken: Long): Unit = ()
  }
}
