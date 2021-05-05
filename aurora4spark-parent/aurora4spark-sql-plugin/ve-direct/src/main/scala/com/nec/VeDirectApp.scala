package com.nec

import com.nec.VeFunction.StackArgument
import com.nec.aurora.Aurora
import me.shadaj.scalapy.interpreter.CPythonInterpreter

import java.nio.file.Paths
import me.shadaj.scalapy.readwrite.Writer
import org.bytedeco.javacpp.{BytePointer, DoublePointer, LongPointer}
import sun.misc.Unsafe

import java.nio.ByteBuffer
import java.time.Instant

object VeDirectApp {

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def main(args: Array[String]): Unit = {
    val proc = Aurora.veo_proc_create(0)
    try {
      val ctx = Aurora.veo_context_open(proc)
      try {
        val ve_so_name = "/home/william/_ve_build/_sum.so"
        val lib = Aurora.veo_load_library(proc, ve_so_name)
        println(s"Lib load = ${lib}")
        val our_args = Aurora.veo_args_alloc()

        /** Put in the raw data */
        val dataDoublePointer = new DoublePointer(3, 6)
        println(s"First result = ${dataDoublePointer.get()}")
        val vmem_pointer = new LongPointer(1)
        val allocRes = Aurora.veo_alloc_mem(proc, vmem_pointer, 8 * 2)
        println(s"Alloc rec = ${allocRes}")
        val ves = Aurora.veo_write_mem(proc, vmem_pointer.get(), dataDoublePointer, 2 * 8)
        println(s"Write mem result = ${ves}")
        val bb = ByteBuffer.allocate(8)
        bb.putLong(vmem_pointer.get())

        val ves2 =
          Aurora.veo_args_set_stack(our_args, 0, 0, dataDoublePointer.asByteBuffer(), 8 * 2)
        val ves3 = Aurora.veo_args_set_i64(our_args, 1, 2)
        println(s"Set args: $ves2, $ves3")

        /** Call */
        try {
          val req_id = Aurora.veo_call_async_by_name(ctx, lib, "sum", our_args)

          println(s"Call async ==> ${req_id}")

          /** Get a result back */
          val longPointer = new LongPointer(8)
          val res3 = Aurora.veo_call_wait_result(ctx, req_id, longPointer)
          println(s"WaitResult = $res3")
          println(longPointer.asByteBuffer().getDouble(0))
        } finally our_args.close()
        ctx.close()
      } finally Aurora.veo_context_close(ctx)
    } finally Aurora.veo_proc_destroy(proc)
  }

  def py_main(args: Array[String]): Unit = {
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
