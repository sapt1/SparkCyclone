package com.nec

import com.nec.VeFunction.StackArgument
import me.shadaj.scalapy.py
import sun.misc.Unsafe

final case class VeCallContext(
  veo: py.Dynamic,
  lib: py.Dynamic,
  ctxt: py.Dynamic,
  unsafe: Unsafe,
  np: py.Dynamic
) {

  /**
   * TODO pass a Java-created pointer rather than a Python-created pointer. This requires direct
   * AVEO API from the JVM
   *
   * This is all the key boilerplate to minimize the size of these function definitions
   */
  def execute[T](
    veFunction: VeFunction[StackArgument],
    ln: Int,
    uploadData: VeFunction[Option[Long]] => Unit,
    loadData: (py.Dynamic, VeFunction[Option[Long]]) => T
  ): T = {

    val numPyBits = veFunction.map {
      case StackArgument.Int => None
      case StackArgument.ListOfDouble =>
        Some(np.zeros(ln, dtype = np.double))
    }

    val rawDataPositions = numPyBits.map {
      _.map { npArr =>
        npArr.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]
      }
    }

    uploadData(rawDataPositions)
    val args: List[py.Any] = ctxt :: numPyBits.args.map {
      case Some(npv) => veo.OnStack(npv, inout = veo.INTENT_INOUT): py.Any
      case None      => ln: py.Any
    }
    val req = lib.applyDynamic(veFunction.name)(args: _*)

    val res = req.wait_result()

    loadData(res, rawDataPositions)

  }
}
