package com.nec

import me.shadaj.scalapy.interpreter.CPythonInterpreter

import java.nio.file.Paths
import me.shadaj.scalapy.py.SeqConverters
import me.shadaj.scalapy.readwrite.Writer
import sun.misc.Unsafe

object VeDirectApp {

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def main(args: Array[String]): Unit = {
    import me.shadaj.scalapy.py
    val veo = py.module("nlcpy.veo")
    val bld = veo.VeBuild()
    bld.set_build_dir("_ve_build")
    bld.set_c_src(
      "_sum",
      """

double sum(double *a, int n)
{
    int i;
    double sum = 0;
    for (i = 0; i < n; i++) {
        sum += a[i];
    }

    return sum;
}

void sum_pairwise(double *a, double *b, double *c, int n)
{
    int i;
    double sum = 0;
    for (i = 0; i < n; i++) {
        c[i] = a[i] + b[i];
    }
}

"""
    )

    val ve_so_name = bld.build_so().as[String]

    val proc = veo.VeoAlloc().proc
    CPythonInterpreter.set("proc", Writer.anyWriter[py.Any].write(proc.as[py.Any]))
    try {
      val ctxt = proc.open_context()
      val lib =
        proc.load_library(py.eval("b\"" + Paths.get(ve_so_name).toAbsolutePath.toString + "\""))
      lib.sum.args_type(py.eval("b\"double*\""), "int")
      lib.sum.ret_type("double")

      lib.sum_pairwise.args_type(
        py.eval("b\"double*\""),
        py.eval("b\"double*\""),
        py.eval("b\"double*\""),
        "int"
      )
      lib.sum_pairwise.ret_type("double")
      val np = veo.np

      val numbers = Seq[Double](1, 2, 3).toPythonProxy
      val numbers2 = Seq[Double](2, 3, 4).toPythonProxy
      val numbers3 = Seq.fill[Double](3)(0).toPythonProxy
      val np_numbers = np.array(numbers)
      val np_numbers2 = np.array(numbers2)
      val np_numbers3 = np.array(numbers3)

      val pos_1 = np_numbers2.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]

      /** Try to set a value here */
      getUnsafe.putDouble(pos_1, 9)
      val b_ve = proc.alloc_mem(py.Dynamic.global.len(numbers) * 8)
      try {
        proc.write_mem(b_ve, np_numbers3, py.Dynamic.global.len(numbers) * 8)
        val req = lib.sum_pairwise(
          ctxt,
          veo.OnStack(np_numbers, inout = veo.INTENT_IN),
          veo.OnStack(np_numbers2, inout = veo.INTENT_IN),
          b_ve,
          py.Dynamic.global.len(numbers)
        )
        req.wait_result()

        proc.read_mem(np_numbers3, b_ve, py.Dynamic.global.len(numbers) * 8)
        val pos = np_numbers3.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]

        List(0, 1, 2)
          .map { num =>
            getUnsafe.getDouble(pos + (8 * num))
          }
          .zipWithIndex
          .foreach { case (v, i) =>
            println(s"Index ${i} => $v")
          }
      } finally { /*proc.free_mem(a_ve) */ }
    } finally CPythonInterpreter.eval("del proc")
  }
}
