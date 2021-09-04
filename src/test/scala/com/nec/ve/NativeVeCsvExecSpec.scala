package com.nec.ve

import com.nec.arrow.functions.CsvParse
import com.nec.aurora.Aurora
import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.VectorEngineNativeEvaluator
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.NativeCsvExec.transformInputStream
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.BeforeAndAfter
import org.scalatest.ConfigMap
import org.scalatest.freespec.AnyFreeSpec

import java.io.ByteArrayInputStream

final class NativeVeCsvExecSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with LazyLogging {

  implicit class RichVeProc(proc: Aurora.veo_proc_handle) {
    def ensureOk(): Aurora.veo_proc_handle = {
      require(proc != null, "Proc could not be allocated, got null")
      require(proc.address() != 0, s"Address for 0 for proc was ${proc}")
      logger.info(s"VE Process = ${proc}")
      proc
    }
  }

  private var initialized = false
  private lazy val proc = {
    initialized = true
    Aurora.veo_proc_create(0).ensureOk()
  }

  override protected def afterAll(configMap: ConfigMap): Unit = {
    super.afterAll(configMap)
    if (initialized) {
      Aurora.veo_proc_destroy(proc)
    }
  }

  "We can parse a CSV with a unix socket + VE" in {
    val (path, compiler) = NativeCompiler.fromTemporaryDirectory(VeCompilerConfig.testConfig)
    val evaluator =
      new VectorEngineNativeEvaluator(proc, compiler).forCode(CsvParse.CsvParseCode)
    val is = new ByteArrayInputStream("a,b,c\n1,2,3\n4.1,5,6\n".getBytes())
    val colBatch =
      transformInputStream(3, 3, evaluator, "test-file", is)(logger)

    val allItems = (0 until colBatch.numRows()).map { rowNum =>
      (
        colBatch.column(0).getDouble(rowNum),
        colBatch.column(1).getDouble(rowNum),
        colBatch.column(2).getDouble(rowNum)
      )
    }.toList

    assert(allItems == List((1d, 2d, 3d), (4.1000000000000005, 5d, 6d)))
  }

}