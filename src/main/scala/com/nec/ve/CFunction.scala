package com.nec.ve

import com.nec.cmake.TcpDebug

final case class CFunction(
                            inputs: List[CVector],
                            outputs: List[CVector],
                            body: CodeLines,
                            hasSets: Boolean = false
                          ) {
  def toCodeLinesSPtr(functionName: String): CodeLines = CodeLines.from(
    "#include <cmath>",
    "#include <bitset>",
    "#include <string>",
    "#include <iostream>",
    "#include <tuple>",
    "#include \"tuple_hash.hpp\"",
    """#include "frovedis/core/radix_sort.hpp"""",
    """#include "frovedis/dataframe/join.hpp"""",
    """#include "frovedis/dataframe/join.cc"""",
    """#include "frovedis/core/set_operations.hpp"""",
    TcpDebug.conditional.headers,
    toCodeLinesNoHeaderOutPtr2(functionName)
  )
  def toCodeLinesS(functionName: String): CodeLines = CodeLines.from(
    "#include <cmath>",
    "#include <bitset>",
    "#include <string>",
    "#include <iostream>",
    "#include <tuple>",
    "#include \"tuple_hash.hpp\"",
    """#include "frovedis/core/radix_sort.hpp"""",
    """#include "frovedis/dataframe/join.hpp"""",
    """#include "frovedis/dataframe/join.cc"""",
    """#include "frovedis/core/set_operations.hpp"""",
    TcpDebug.conditional.headers,
    toCodeLinesNoHeader(functionName)
  )

  def arguments: List[CVector] = inputs ++ outputs

  def toCodeLinesPF(functionName: String): CodeLines = {
    CodeLines.from(
      "#include <cmath>",
      "#include <bitset>",
      "#include <string>",
      "#include <iostream>",
      TcpDebug.conditional.headers,
      toCodeLinesNoHeader(functionName)
    )
  }
  def toCodeLinesG(functionName: String): CodeLines = {
    CodeLines.from(
      "#include <cmath>",
      "#include <bitset>",
      "#include <string>",
      "#include <iostream>",
      "#include <tuple>",
      "#include \"tuple_hash.hpp\"",
      TcpDebug.conditional.headers,
      toCodeLinesNoHeader(functionName)
    )
  }
  def toCodeLinesJ(functionName: String): CodeLines = {
    CodeLines.from(
      "#include <cmath>",
      "#include <bitset>",
      "#include <string>",
      "#include <iostream>",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/dataframe/join.cc"""",
      """#include "frovedis/core/set_operations.hpp"""",
      TcpDebug.conditional.headers,
      toCodeLinesNoHeader(functionName)
    )
  }

  def toCodeLinesNoHeader(functionName: String): CodeLines = {
    CodeLines.from(
      s"""extern "C" long $functionName(""",
      arguments
        .map { cVector =>
          s"${cVector.veType.cVectorType} *${cVector.name}"
        }
        .mkString(",\n"),
      ") {",
      body.indented,
      "  ",
      "  return 0;",
      "};"
    )
  }

  def toCodeLinesNoHeaderOutPtr(functionName: String): CodeLines = {
    CodeLines.from(
      s"""extern "C" long $functionName(""", {
        inputs
          .map { cVector =>
            s"${cVector.veType.cVectorType} **${cVector.name}"
          } ++ { if (hasSets) List("int *sets") else Nil } ++
          outputs
            .map { cVector =>
              s"${cVector.veType.cVectorType} **${cVector.name}"
            }
      }
        .mkString(",\n"),
      ") {",
      body.indented,
      "  ",
      "  return 0;",
      "};"
    )
  }

  def toCodeLinesNoHeaderOutPtr2(functionName: String): CodeLines = {
    CodeLines.from(
      s"""extern "C" long $functionName(""", {
        List(
          inputs
            .map { cVector =>
              s"${cVector.veType.cVectorType} **${cVector.name}_m"
            },
          if (hasSets) List("int *sets") else Nil,
          outputs
            .map { cVector =>
              s"${cVector.veType.cVectorType} **${cVector.name}_mo"
            }
        ).flatten
      }
        .mkString(",\n"),
      ") {",
      CodeLines
        .from(
          CodeLines.debugHere,
          inputs.map { cVector =>
            CodeLines.from(
              s"${cVector.veType.cVectorType}* ${cVector.name} = ${cVector.name}_m[0];"
            )
          },
          CodeLines.debugHere,
          outputs.map { cVector =>
            CodeLines.from(
              s"${cVector.veType.cVectorType}* ${cVector.name} = (${cVector.veType.cVectorType} *)malloc(sizeof(${cVector.veType.cVectorType}));",
              s"*${cVector.name}_mo = ${cVector.name};"
            )
          },
          CodeLines.debugHere,
          body
        )
        .indented,
      "  ",
      "  return 0;",
      "};"
    )
  }
}