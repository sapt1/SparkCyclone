package com.nec.native
import com.nec.arrow.TransferDefinitions
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.Paths
import com.nec.cmake.CMakeBuilder
import com.nec.native.NativeCompiler.Program
import org.apache.spark.SparkConf

import java.nio.file.Files
import java.nio.file.Path
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.VeCompilerConfig

trait NativeCompiler extends Serializable {

  /** Location of the compiled kernel library */
  def forProgram(program: Program): Path

  protected def combinedCode(code: String): String =
    List(TransferDefinitions.TransferDefinitionsSourceCode, code).mkString("\n\n")
}

object NativeCompiler {

  final case class Program(code: String, defines: Defines)

  object Program {
    def debug(cCode: String): Program = Program(cCode, Defines.debug)
  }

  final case class Defines(values: Map[String, String]) {
    def ++(defines: Defines): Defines = Defines(values ++ defines.values)

    def toArgsList: List[String] = values.flatMap { case (k, v) => List("-D", s"$k=$v") }.toList
  }

  object Defines {
    def debug: Defines = Defines(Map("DEBUG" -> "1"))

    def empty: Defines = Defines(values = Map.empty)
  }

  def fromConfig(sparkConf: SparkConf): NativeCompiler = {
    val compilerConfig = VeKernelCompiler.VeCompilerConfig.fromSparkConf(sparkConf)
    sparkConf.getOption("spark.com.nec.spark.kernel.precompiled") match {
      case Some(directory) => PreCompiled(directory)
      case None =>
        sparkConf.getOption("spark.com.nec.spark.kernel.directory") match {
          case Some(directory) =>
            OnDemandCompilation(directory, compilerConfig)
          case None =>
            fromTemporaryDirectory(compilerConfig)._2
        }
    }
  }

  def fromTemporaryDirectory(compilerConfig: VeCompilerConfig): (Path, NativeCompiler) = {
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    (tmpBuildDir, OnDemandCompilation(tmpBuildDir.toAbsolutePath.toString, compilerConfig))
  }

  final case class CachingNativeCompiler(
    nativeCompiler: NativeCompiler,
    var cache: Map[Program, Path] = Map.empty
  ) extends NativeCompiler
    with LazyLogging {

    /** Location of the compiled kernel library */
    override def forProgram(program: Program): Path = this.synchronized {
      cache.get(program) match {
        case None =>
          logger.debug(s"Cache miss for compilation.")
          val compiledPath = nativeCompiler.forProgram(program)
          cache = cache.updated(program, compiledPath)
          compiledPath
        case Some(path) =>
          logger.debug(s"Cache hit for compilation.")
          path
      }
    }
  }

  final case class OnDemandCompilation(buildDir: String, veCompilerConfig: VeCompilerConfig)
    extends NativeCompiler
    with LazyLogging {
    override def forProgram(program: Program): Path = {
      val cc = combinedCode(program.code)
      val sourcePath =
        Paths.get(buildDir).resolve(s"_spark_${(cc, program).hashCode}.so").toAbsolutePath

      if (sourcePath.toFile.exists()) {
        logger.debug(s"Loading precompiled from path: $sourcePath")
        sourcePath
      } else {
        logger.debug(s"Compiling for the VE...: $program")
        val startTime = System.currentTimeMillis()
        val soName =
          VeKernelCompiler(
            compilationPrefix = s"_spark_${cc.hashCode}",
            Paths.get(buildDir),
            veCompilerConfig
          )
            .compile_c(Program(cc, program.defines))
        val endTime = System.currentTimeMillis() - startTime
        logger.debug(s"Compiled code in ${endTime}ms to path ${soName}.")
        soName
      }
    }
  }

  final case class PreCompiled(sourceDir: String) extends NativeCompiler with LazyLogging {
    override def forProgram(program: Program): Path = {
      val cc = combinedCode(program.code)
      val sourcePath =
        Paths.get(sourceDir).resolve(s"_spark_${(cc, program).hashCode}.so").toAbsolutePath
      logger.debug(s"Will be loading source from path: $sourcePath")
      sourcePath
    }
  }

  object CNativeCompiler extends NativeCompiler {
    override def forProgram(program: Program): Path = {
      CMakeBuilder.buildCLogging(
        Program(
          code = List(TransferDefinitions.TransferDefinitionsSourceCode, program.code)
            .mkString("\n\n"),
          defines = program.defines
        )
      )
    }
  }

}
