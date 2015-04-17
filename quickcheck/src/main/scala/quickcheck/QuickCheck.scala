package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import Math._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == min(a, b)
  }

  property("min3") = forAll { (a: Int, b: Int, c: Int) =>
    val h = insert(c, insert(b, insert(a, empty)))
    findMin(h) == min(a, min(b, c))
  }

  property("delete1") = forAll { a: Int =>
    isEmpty(deleteMin(insert(a, empty)))
  }

  property("delete2") = forAll { (a: Int, b: Int) =>
    !isEmpty(deleteMin(insert(b, insert(a, empty))))
  }

  property("delete3") = forAll { (a: Int, b: Int, c: Int) =>
    val h = insert(c, insert(b, insert(a, empty)))
    findMin(deleteMin(h)) >= findMin(h) &&
      findMin(deleteMin(deleteMin(h))) >= findMin(deleteMin(h))
  }

  property("gen1") = forAll { h: H =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("gen2") = forAll { (a: H, b: H) =>
    if (!isEmpty(a) && !isEmpty(b))
      findMin(meld(a, b)) == min(findMin(a), findMin(b))
    else true
  }

  property("gen3") = forAll { a: H =>
    val lst = returnHeap(a, Nil)
    lst == lst.sorted.reverse
  }

  property("gen4") = forAll { (a: H, b: H) =>
    val lst = returnHeap(meld(a, b), Nil)
    lst == lst.sorted.reverse
  }

  property("gen5") = forAll { (a: H, b: H) =>
    if (isEmpty(a) && isEmpty(b))
      true
    else {
      val m = if (!isEmpty(a))
        findMin(a)
      else if (!isEmpty(b))
        findMin(b)
      val lst = returnHeap(meld(a, b), Nil)
      lst.contains(m)
    }
  }

  private def returnHeap(h: H, s: List[A]): List[A] =
    if (isEmpty(h)) s else returnHeap(deleteMin(h), findMin(h) :: s)

  lazy val genHeap: Gen[H] = frequency((1, emptyHeap), (2, nonEmptyHeap))

  def emptyHeap = const(empty)

  def nonEmptyHeap = for {
    elt <- arbitrary[A]
    heap <- genHeap
  } yield insert(elt, heap)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
