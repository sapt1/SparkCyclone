package com.nec.cmake

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.planning.Tracer
import org.scalatest.freespec.AnyFreeSpec

class TracerTest extends AnyFreeSpec {
  def includeUdp: Boolean = false
  lazy val evaluator: NativeEvaluator = NativeEvaluator.CNativeEvaluator
  "We can trace" in {
    val functionName = "test"
    val ani = evaluator.forCode(code =
      CodeLines
        .from(
          Tracer.DefineTracer.cCode,
          if (includeUdp) UdpDebug.default.headers else CodeLines.empty,
          CFunction(
            inputs = List(Tracer.TracerVector),
            outputs = Nil,
            body = CodeLines.from(
              if (includeUdp) UdpDebug.default.createSock else CodeLines.empty,
              CodeLines.debugHere,
              if (includeUdp) UdpDebug.default.close else CodeLines.empty
            )
          )
            .toCodeLinesNoHeader(functionName)
            .cCode
        )
        .cCode
    )
    WithTestAllocator { implicit allocator =>
      val inVec = Tracer.Launched("launchId").map("mappingId")
      val vec = inVec.createVector()
      try {
        ani.callFunctionWrapped(functionName, List(NativeArgument.input(vec)))
      } finally vec.close()
    }
  }
}