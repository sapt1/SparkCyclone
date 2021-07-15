package com.nec.spark.planning

import com.nec.arrow.ExecutorDeferredVeArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.compile_cpp
import org.apache.spark.SparkConf
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.Files

final class LocalVeoNativeEvaluator(sparkConf: SparkConf) extends NativeEvaluator with LazyLogging {
  override def forCode(code: String): ArrowNativeInterfaceNumeric = {
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    logger.debug(s"Compiling for the VE...: ${code}")
    val startTime = System.currentTimeMillis()
    val soName = compile_cpp(
      buildDir = tmpBuildDir,
      config = VeKernelCompiler.VeCompilerConfig.fromSparkConf(sparkConf),
      List(TransferDefinitions.TransferDefinitionsSourceCode, code).mkString("\n\n")
    ).toAbsolutePath.toString
    val endTime = System.currentTimeMillis() - startTime
    logger.debug(s"Compiled code in ${endTime}ms to path ${soName}.")

    ExecutorDeferredVeArrowNativeInterfaceNumeric(soName)
  }
}
