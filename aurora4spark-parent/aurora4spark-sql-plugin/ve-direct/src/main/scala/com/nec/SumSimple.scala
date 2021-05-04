package com.nec

import com.nec.VeFunction.StackArgument

object SumSimple {
  val C_Definition =
    """
      |double sum(double *a, int n)
      |{
      |    int i;
      |    double sum = 0;
      |    for (i = 0; i < n; i++) {
      |        sum += a[i];
      |    }
      |
      |    return sum;
      |}
      |""".stripMargin

  val Ve_F = VeFunction(
    name = "sum",
    args = List[StackArgument](StackArgument.ListOfDouble, StackArgument.Int),
    ret_type = Some("'int'")
  )

  /**
   * Currently, this is the minimum code needed to implement SumPairwise -- we can do better more
   * generically, but this will suffice for now.
   */
  def sumSimple(veCallContext: VeCallContext, inputs: List[Double]): Double = {
    veCallContext.execute(
      veFunction = Ve_F,
      ln = inputs.length,
      uploadData = { poss =>
        inputs.iterator.zipWithIndex.foreach { case (a, idx) =>
          poss.args(0).foreach { Pos_In_1 =>
            veCallContext.unsafe.putDouble(Pos_In_1 + idx * 8, a)
          }
        }
      },
      loadData = { (ret, poss) =>
        ret.as[Double]
      }
    )
  }
}
