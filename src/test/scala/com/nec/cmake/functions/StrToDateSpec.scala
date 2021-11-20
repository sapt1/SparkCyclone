package com.nec.cmake.functions

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.RichVectors._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.Checkers

final class StrToDateSpec extends AnyFreeSpec with Checkers {
  "it works" in {
    val cLib = CMakeBuilder.buildCLogging(
      List(
        TransferDefinitionsSourceCode,
        "\n\n",
        CFunction(
          inputs = List(CVector.varChar("input_0")),
          outputs = List(CVector.int("output_0")),
          body = CodeLines.from(
            GroupByOutline.initializeScalarVector(VeScalarType.veNullableInt, "output_0", "1"),
            """output_0->data[0] = str_to_date(input_0, 0);""",
            "set_validity(output_0->validityBuffer, 0, 1);",
            "return 0;"
          )
        ).toCodeLines("test").cCode
      )
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      ArrowVectorBuilders.withArrowStringVector(Seq("1995-03-15")) { inVec =>
        ArrowVectorBuilders.withDirectIntVector(Seq.empty) { outVec =>
          nativeInterface.callFunction(
            name = "test",
            inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
            outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
          )
          assert(outVec.toList == List(9204))
        }
      }
    }
  }
}
