package com.nec

import com.nec.VeFunction.StackArgument
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
      s"""

${SumSimple.C_Definition}
${SumPairwise.C_Definition}
"""
    )

    val ve_so_name = bld.build_so().as[String]

    val proc = veo.VeoAlloc().proc
    CPythonInterpreter.set("proc", Writer.anyWriter[py.Any].write(proc.as[py.Any]))

    import SumPairwise.{Ve_F => sp}

    val VeFunctions = Map[String, VeFunction[StackArgument]](sp.name -> sp)

    try {
      val ctxt = proc.open_context()
      val lib =
        proc.load_library(py.eval("b\"" + Paths.get(ve_so_name).toAbsolutePath.toString + "\""))

      lib.sum.args_type(py.eval("b\"double*\""), "int")
      lib.sum.ret_type("double")

      VeFunctions.foreach { case (_, VeFunction(name, args, ret_type)) =>
        lib
          .selectDynamic(name)
          .applyDynamic("args_type")(args.map(v => py.eval(v.tpe)): _*)
        ret_type.foreach(r => lib.selectDynamic(name).applyDynamic("ret_type")(r))
      }

      val np: py.Dynamic = veo.np

      val veCallContext = VeCallContext(veo, lib, ctxt, getUnsafe, np)

      val small: List[(Double, Double)] = List((1, 2), (2, 4), (5, 9))
      val large = List.fill[(Double, Double)](5000)(
        (scala.util.Random.nextDouble(), scala.util.Random.nextDouble())
      )
      println(Instant.now())
      println(SumSimple.sumSimple(veCallContext, List(1, 2, 3)))
      println(Instant.now())
      println(SumPairwise.sumPairwise(veCallContext, small))
      println(Instant.now())
      println(SumPairwise.sumPairwise(veCallContext, large).take(50))
      println(Instant.now())
    } finally CPythonInterpreter.eval("del proc")
  }
}
