package com.nec

import com.nec.VeCallContext.{IntArgument, ListDoubleArgument}
import com.nec.VeFunction.StackArgument
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.{DoublePointer, LongPointer}

object AvgSimple {
  val C_Definition =
    """
      |double avg(double *a, int n)
      |{
      |    int i;
      |    double sum = 0;
      |    for (i = 0; i < n; i++) {
      |        sum += a[i];
      |    }
      |
      |    return sum/i;
      |}
      |""".stripMargin

  val Ve_F = VeFunction(
    name = "avg",
    args = List[StackArgument](StackArgument.ListOfDouble, StackArgument.Int),
    ret_type = Some("'double'")
  )

  def avg_doubles(veJavaContext: VeJavaContext, doubles: List[Double]): Double = {
    val our_args = Aurora.veo_args_alloc()

    import veJavaContext._

    /** Put in the raw data */
    val dataDoublePointer = new DoublePointer(doubles: _*)
    Aurora.veo_args_set_stack(our_args, 0, 0, dataDoublePointer.asByteBuffer(), 8 * doubles.length)
    Aurora.veo_args_set_i64(our_args, 1, doubles.length)

    /** Call */
    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "avg", our_args)
      val longPointer = new LongPointer(8)
      Aurora.veo_call_wait_result(ctx, req_id, longPointer)
      longPointer.asByteBuffer().getDouble(0)
    } finally our_args.close()
  }
}