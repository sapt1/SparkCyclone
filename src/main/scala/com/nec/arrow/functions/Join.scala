package com.nec.arrow.functions
import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.Float8VectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.IntVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.Float8VectorOutputWrapper
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.Float8Vector

object Join {

  val JoinSourceCode: String = {
    val source = scala.io.Source.fromInputStream(
      getClass.getResourceAsStream("/com/nec/arrow/functions/cpp/joiner.cc")
    )
    try source.mkString
    finally source.close()
  }

  def runOn(nativeInterface: ArrowNativeInterface)(
    leftValuesVector: Float8Vector,
    rightValuesVector: Float8Vector,
    leftKeyVector: IntVector,
    rightKeyVector: IntVector,
    outputVector: Float8Vector
  ): Unit = {

    nativeInterface.callFunctionWrapped(
      "join_doubles",
      List(
        NativeArgument.VectorInputNativeArgument(Float8VectorInputWrapper(leftValuesVector)),
        NativeArgument.VectorInputNativeArgument(Float8VectorInputWrapper(rightValuesVector)),
        NativeArgument.VectorInputNativeArgument(IntVectorInputWrapper(leftKeyVector)),
        NativeArgument.VectorInputNativeArgument(IntVectorInputWrapper(rightKeyVector)),
        NativeArgument.VectorOutputNativeArgument(Float8VectorOutputWrapper(outputVector))
      )
    )
  }

  def joinJVM(
    leftColumn: Float8Vector,
    rightColumn: Float8Vector,
    leftKey: IntVector,
    rightKey: IntVector
  ): List[(Double, Double)] = {
    val leftColVals = (0 until leftColumn.getValueCount).map(idx => leftColumn.get(idx))
    val rightColVals = (0 until rightColumn.getValueCount).map(idx => rightColumn.get(idx))
    val leftKeyVals = (0 until leftKey.getValueCount).map(idx => leftKey.get(idx))
    val rightKeyVals = (0 until rightKey.getValueCount).map(idx => rightKey.get(idx))
    val leftMap = leftKeyVals.zip(leftColVals).toMap
    val rightMap = rightKeyVals.zip(rightColVals).toMap
    val joinedKeys = leftKeyVals.filter(key => rightMap.contains(key))
    joinedKeys.map(key => leftMap(key)).zip(joinedKeys.map(key => rightMap(key))).toList
  }
}
