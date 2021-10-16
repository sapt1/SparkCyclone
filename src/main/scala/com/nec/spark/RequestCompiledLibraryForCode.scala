package com.nec.spark

import com.nec.native.NativeCompiler.Program

final case class RequestCompiledLibraryForCode(program: Program) extends Serializable {}
