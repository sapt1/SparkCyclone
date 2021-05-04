package com.nec

import me.shadaj.scalapy.interpreter.CPythonInterpreter

import java.nio.file.Paths
import me.shadaj.scalapy.readwrite.Writer
import sun.misc.Unsafe

import java.time.Instant

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

      def sumPairwise(inputs: List[(Double, Double)]): List[Double] = {

        val ln = inputs.length

        /**
         * TODO pass a Java-created pointer rather than a Python-created pointer. This requires
         * direct AVEO API from the JVM
         */
        val np_In_1 = np.zeros(ln, dtype = np.double)
        val np_In_2 = np.zeros(ln, dtype = np.double)
        val np_Out_1 = np.zeros(ln, dtype = np.double)

        val Pos_In_1 = np_In_1.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]
        val Pos_In_2 = np_In_2.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]
        val Pos_Out_1 = np_Out_1.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]

        inputs.iterator.zipWithIndex.foreach { case ((a, b), idx) =>
          getUnsafe.putDouble(Pos_In_1 + idx * 8, a)
          getUnsafe.putDouble(Pos_In_2 + idx * 8, b)
        }

        val Pos_Out_1_VE = proc.alloc_mem(ln * 8)
        try {
          val req = lib.sum_pairwise(
            ctxt,
            veo.OnStack(np_In_1, inout = veo.INTENT_IN),
            veo.OnStack(np_In_2, inout = veo.INTENT_IN),
            Pos_Out_1_VE,
            ln
          )
          req.wait_result()

          proc.read_mem(np_Out_1, Pos_Out_1_VE, ln * 8)

          inputs.indices.iterator.map { num =>
            getUnsafe.getDouble(Pos_Out_1 + (8 * num))
          }.toList
        } finally {
          proc.free_mem(Pos_Out_1_VE)
        }
      }

      val small: List[(Double, Double)] = List((1, 2), (2, 4), (5, 9))
      val large = List.fill[(Double, Double)](5000)(
        (scala.util.Random.nextDouble(), scala.util.Random.nextDouble())
      )
      println(Instant.now())
      println(sumPairwise(small))
      println(Instant.now())
      println(sumPairwise(large).take(50))
      println(Instant.now())
    } finally CPythonInterpreter.eval("del proc")
  }
}
