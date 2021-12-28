package com.nec.ve

import com.nec.cmake.TcpDebug
import com.nec.spark.agile.CExpressionEvaluation.shortenLines
import com.nec.spark.agile.CFunctionGeneration.CExpression
import com.nec.spark.planning.Tracer
import com.nec.spark.planning.Tracer.{TracerDefName, TracerOutput}
import com.nec.ve.VeType.VeScalarType

final case class CodeLines(lines: List[String]) {
  def time(name: String): CodeLines = {
    val udpdebug: List[String] =
      List("utcnanotime().c_str()", """" $ """") ++ TracerOutput ++ List(
        """" $$ """",
        s""""S:${name}:L"""",
        "__LINE__",
        "std::endl"
      )
    val udpdebugE: List[String] =
      List("utcnanotime().c_str()", """" $ """") ++ TracerOutput ++ List(
        """" $$ """",
        s""""E:${name}:L"""",
        "__LINE__",
        "std::endl"
      )
    CodeLines.from(
      TcpDebug
        .Conditional(TracerDefName, TcpDebug.conditional)
        .send(udpdebug: _*),
      this,
      TcpDebug
        .Conditional(TracerDefName, TcpDebug.conditional)
        .send(udpdebugE: _*)
    )
  }

  def ++(other: CodeLines): CodeLines = CodeLines(lines = lines ++ (" " :: other.lines))

  def block: CodeLines = CodeLines.from("", "{", this.indented, "}", "")

  def blockCommented(str: String): CodeLines =
    CodeLines.from(s"// ${str}", "{", this.indented, "}", "")

  def indented: CodeLines = CodeLines(lines = lines.map(line => s"  $line"))

  override def toString: String =
    (List(s"CodeLines(") ++ shortenLines(lines) ++ List(")")).mkString("\n")

  def cCode: String = lines.mkString("\n", "\n", "\n")

  def append(codeLines: CodeLines*): CodeLines = copy(lines = lines ++ codeLines.flatMap(_.lines))
}
object CodeLines {

  def debugValue(names: String*): CodeLines =
    TcpDebug.conditionOn("DEBUG")(
      CodeLines
        .from(s"std::cout << ${names.mkString(" << \" \" << ")} << std::endl << std::flush;")
    )

  def printLabel(label: String): CodeLines = {
    val parts = s""""$label"""" :: Nil
    CodeLines
      .from(s"std::cout << ${parts.mkString(" << \" \" << ")} << std::endl << std::flush;")
  }

  def printValue(label: String)(names: String*): CodeLines = {
    val parts = s""""$label"""" :: names.toList
    CodeLines
      .from(s"std::cout << ${parts.mkString(" << \" \" << ")} << std::endl << std::flush;")
  }

  def debugHere(implicit fullName: sourcecode.FullName, line: sourcecode.Line): CodeLines = {
    val startdebug: List[String] = List("utcnanotime().c_str()", """" $ """")
    val enddebug: List[String] =
      List("""" $$ """", s""""${fullName.value}#${line.value}/#"""", "__LINE__", "std::endl")

    val udpdebug: List[String] = startdebug ++ TracerOutput ++ enddebug

    val debugInfo = startdebug ++ enddebug
    CodeLines.from(
      TcpDebug
        .Conditional(TracerDefName, TcpDebug.conditional)
        .send(udpdebug: _*),
      TcpDebug.conditionOn("DEBUG")(
        CodeLines.from(s"""std::cout ${Tracer.concatStr(debugInfo)} << std::flush;""")
      )
    )
  }

  def commentHere(
    what: String*
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): CodeLines =
    CodeLines.from(
      what.map(w => CodeLines.from(s"// $w")).toList,
      s"// ${fullName.value} (#${line.value})"
    )

  def from(str: CodeLines*): CodeLines = CodeLines(lines = str.flatMap(_.lines).toList)

  def ifStatement(condition: String)(sub: => CodeLines): CodeLines =
    CodeLines.from(s"if ($condition) { ", sub.indented, "}")
  def ifElseStatement(condition: String)(sub: => CodeLines)(other: CodeLines): CodeLines =
    CodeLines.from(s"if ($condition) { ", sub.indented, "} else {", other.indented, "}")
  def forLoop(counterName: String, until: String)(sub: => CodeLines): CodeLines =
    CodeLines.from(
      s"for ( int $counterName = 0; $counterName < $until; $counterName++ ) {",
      sub.indented,
      s"}"
    )

  implicit def stringToCodeLines(str: String): CodeLines = CodeLines(List(str))

  implicit def listStringToCodeLines(str: List[String]): CodeLines = CodeLines(str)

  implicit def listCodeLines(str: List[CodeLines]): CodeLines = CodeLines(str.flatMap(_.lines))

  def empty: CodeLines = CodeLines(Nil)

  def initializeStringVector(variableName: String): CodeLines = CodeLines.empty

  def debugVector(name: String): CodeLines = {
    CodeLines.from(
      s"for (int i = 0; i < $name->count; i++) {",
      CodeLines.from(
        s"""std::cout << "${name}[" << i << "] = " << ${name}->data[i] << " (valid? " << check_valid(${name}->validityBuffer, i) << ")" << std::endl << std::flush; """
      ),
      "}"
    )
  }

  def dealloc(cv: CVector): CodeLines = CodeLines.empty

  def declare(cv: CVector): CodeLines = CodeLines.from(
    s"${cv.veType.cVectorType} *${cv.name} = (${cv.veType.cVectorType}*)malloc(sizeof(${cv.veType.cVectorType}));"
  )
  def storeTo(outputName: String, cExpression: CExpression, idx: String): CodeLines =
    cExpression.isNotNullCode match {
      case None =>
        CodeLines.from(
          s"""$outputName->data[${idx}] = ${cExpression.cCode};""",
          s"set_validity($outputName->validityBuffer, ${idx}, 1);"
        )
      case Some(notNullCheck) =>
        CodeLines.from(
          s"if ( $notNullCheck ) {",
          CodeLines
            .from(
              s"""$outputName->data[${idx}] = ${cExpression.cCode};""",
              s"set_validity($outputName->validityBuffer, ${idx}, 1);"
            )
            .indented,
          "} else {",
          CodeLines.from(s"set_validity($outputName->validityBuffer, ${idx}, 0);").indented,
          "}"
        )
    }

  def initializeScalarVector(
    veScalarType: VeScalarType,
    variableName: String,
    countExpression: String
  ): CodeLines =
    CodeLines.from(
      s"$variableName->count = ${countExpression};",
      s"$variableName->data = (${veScalarType.cScalarType}*) malloc($variableName->count * sizeof(${veScalarType.cScalarType}));",
      s"$variableName->validityBuffer = (uint64_t *) malloc(ceil(${countExpression} / 64.0) * sizeof(uint64_t));"
    )

  def scalarVectorFromStdVector(
    veScalarType: VeScalarType,
    targetName: String,
    sourceName: String
  ): CodeLines =
    CodeLines.from(
      s"$targetName = (${veScalarType.cVectorType}*)malloc(sizeof(${veScalarType.cVectorType}));",
      initializeScalarVector(veScalarType, targetName, s"$sourceName.size()"),
      s"for ( int x = 0; x < $sourceName.size(); x++ ) {",
      CodeLines
        .from(
          s"$targetName->data[x] = $sourceName[x];",
          s"set_validity($targetName->validityBuffer, x, 1);"
        )
        .indented,
      "}"
    )

}
