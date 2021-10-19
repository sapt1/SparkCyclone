package com.nec.cmake.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichVarCharVector
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector}
import com.nec.spark.agile.StringProducer.{FilteringProducer, FrovedisCopyStringProducer}
import org.scalatest.freespec.AnyFreeSpec

object FrovedisStringProducerSpec {

  val generatedSource = CFunction(
    inputs = List(CVector.varChar("input")),
    outputs = List(CVector.varChar("output")),
    body = {
      val items = List(0, 1)
      val fp = FilteringProducer("output", FrovedisCopyStringProducer("input"))
      CodeLines.from(
        s"int groups_count = ${items.size};",
        fp.setup,
        "int g;",
        "int i;",
        items.map { i =>
          CodeLines.from(s"g = ${i};", s"i = ${i};", fp.forEach)
        },
        fp.complete,
        items.map { i => CodeLines.from(s"i = ${i};", fp.validityForEach("i")) },
        "varchar_vector_to_words(output);",
        CodeLines.debugHere
      )
    }
  )

  val functionName = "xyz"

  def cLib = CMakeBuilder.buildCLogging(
    cSource = List(
      TransferDefinitionsSourceCode,
      "\n\n",
      generatedSource.toCodeLinesNoHeader(functionName).cCode
    )
      .mkString("\n\n"),
    debug = true
  )

  def getIt(input: Seq[String]): List[String] = {
    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    ArrowVectorBuilders.withArrowStringVector(input) { vcv =>
      ArrowVectorBuilders.withArrowStringVector(Seq.empty) { nvcv =>
        nativeInterface.callFunctionWrapped(
          functionName,
          List(NativeArgument.input(vcv), NativeArgument.output(nvcv))
        )

        nvcv.toList
      }
    }
  }
}
final class FrovedisStringProducerSpec extends AnyFreeSpec {
  "It works" in {
    expect(FrovedisStringProducerSpec.getIt(Seq("x", "yy", "ax", "x")) == List("x", "yy", "ax"))
  }
  "It works (2)" ignore {
    expect(FrovedisStringProducerSpec.getIt(Seq("x", "yy", "ax")) == List("x", "yy", "ax"))
  }
}
