package com.nec

import com.nec.VeFunction.StackArgument

object SumPairwise {
  val C_Definition = """
                       |void sum_pairwise(double *a, double *b, double *c, int n)
                       |{
                       |    int i;
                       |    double sum = 0;
                       |    for (i = 0; i < n; i++) {
                       |        c[i] = a[i] + b[i];
                       |    }
                       |}
                       |""".stripMargin

  val Ve_F = VeFunction(
    name = "sum_pairwise",
    args = List[StackArgument](
      StackArgument.ListOfDouble,
      StackArgument.ListOfDouble,
      StackArgument.ListOfDouble,
      StackArgument.Int
    ),
    ret_type = None
  )

  /**
   * Currently, this is the minimum code needed to implement SumPairwise -- we can do better more
   * generically, but this will suffice for now.
   */
  def sumPairwise(veCallContext: VeCallContext, inputs: List[(Double, Double)]): List[Double] = {
    veCallContext.execute(
      veFunction = Ve_F,
      ln = inputs.length,
      uploadData = { rawDataPositions =>
        inputs.iterator.zipWithIndex.foreach { case ((a, b), idx) =>
          rawDataPositions.args(0).foreach { Pos_In_1 =>
            veCallContext.unsafe.putDouble(Pos_In_1 + idx * 8, a)
          }
          rawDataPositions.args(1).foreach { Pos_In_2 =>
            veCallContext.unsafe.putDouble(Pos_In_2 + idx * 8, b)
          }
        }
      },
      loadData = { (ret_v, poss) =>
        inputs.indices.iterator.map { num =>
          veCallContext.unsafe.getDouble(poss.args.flatten.last + (8 * num))
        }.toList
      }
    )
  }
}
