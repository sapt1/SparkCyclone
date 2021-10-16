package com.nec.native

import com.nec.arrow.ArrowNativeInterface.DeferredArrowInterface
import com.nec.arrow.{ArrowNativeInterface, CArrowNativeInterface}
import com.nec.arrow.VeArrowNativeInterface.VeArrowNativeInterfaceLazyLib
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler.{CNativeCompiler, Defines, Program}
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.typesafe.scalalogging.LazyLogging

trait NativeEvaluator extends Serializable {
  def forProgram(program: Program): ArrowNativeInterface
  final def forCode(code: String): ArrowNativeInterface = forProgram(Program(code, Defines.empty))
}

object NativeEvaluator {

  /** Selected when running in CMake mode */
  object CNativeEvaluator extends NativeEvaluator {
    override def forProgram(program: Program): ArrowNativeInterface = {
      new CArrowNativeInterface(CNativeCompiler.forProgram(program).toAbsolutePath.toString)
    }
  }

  final case class CNativeEvaluator(defines: Defines) extends NativeEvaluator {
    override def forProgram(program: Program): ArrowNativeInterface =
      new CArrowNativeInterface(
        CNativeCompiler
          .forProgram(Program(program.code, defines ++ program.defines))
          .toAbsolutePath
          .toString
      )
  }

  final class VectorEngineNativeEvaluator(
    proc: Aurora.veo_proc_handle,
    nativeCompiler: NativeCompiler
  ) extends NativeEvaluator
    with LazyLogging {
    override def forProgram(program: Program): ArrowNativeInterface = {
      val localLib = nativeCompiler.forProgram(program).toString
      logger.debug(s"For evaluation, will use local lib '$localLib'")
      new VeArrowNativeInterfaceLazyLib(proc, localLib)
    }
  }

  case object ExecutorPluginManagedEvaluator extends NativeEvaluator with LazyLogging {
    def forProgram(program: Program): ArrowNativeInterface = {
      // defer because we need the executors to initialize first
      logger.debug(s"For evaluation, will refer to the Executor Plugin")
      DeferredArrowInterface(() =>
        new VeArrowNativeInterfaceLazyLib(
          Aurora4SparkExecutorPlugin._veo_proc,
          Aurora4SparkExecutorPlugin.libraryStorage.getLocalLibraryPath(program).toString
        )
      )
    }
  }

}
