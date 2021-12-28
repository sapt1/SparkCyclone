package com.nec.testing
import com.nec.testing.ProductListEquivalenceCheck._
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.freespec.AnyFreeSpec

final class ProductListEquivalenceCheck extends AnyFreeSpec {
  "A list of classes with some Doubles is equivalent" in {
    assert(
      listEq.areEqual(List[(String, Double)](("a", 0.0005)), List[(String, Double)](("a", 0.0003)))
    )
  }
  "When values are too far apart, it is no longer equivalent" in {
    assert(
      !listEq.areEqual(List[(String, Double)](("a", 1.0005)), List[(String, Double)](("a", 0.0003)))
    )
  }
}

object ProductListEquivalenceCheck {

  val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-2)
  val twoProductsEq: Equality[Product] = (aProduct: Product, _b: Any) => {
    val bProduct = _b.asInstanceOf[Product]
    aProduct.productArity == bProduct.productArity &&
    aProduct.productIterator.zip(bProduct.productIterator).forall {
      case (a: Double, b: Double) =>
        doubleEq.areEquivalent(a, b)
      case (a, b) =>
        a == b
    }
  }
  val listEq: Equality[List[Product]] = (a: List[Product], _b: Any) => {
    val b = _b.asInstanceOf[List[Product]]
    (a.isEmpty && b.isEmpty) || ((a.size == b.size) && a.zip(b).forall {
      case (aProduct, bProduct) =>
        twoProductsEq.areEqual(aProduct, bProduct)
    })
  }
}
