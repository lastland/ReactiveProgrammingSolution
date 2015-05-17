package suggestions



import language.postfixOps
import scala.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }
    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable.just("erik", "erik meijer", "martin", "a b c")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true
    )
    assert(completed && count == 4, "completed: " + completed + ", event count: " + count)
  }
  test("WikipediaApi should recover exceptions") {
    val ex = new RuntimeException
    val ob = Observable.just(1, 2, 3, 4) map { elt =>
      if (3 == elt) throw ex
      elt
    }
    val recovered = ob.recovered

    val result = mutable.Buffer[Try[Int]]()
    val sub = recovered subscribe {
      result += _
    }
    assert(result == Seq(Success(1), Success(2), Failure(ex)))
  }
  test("WikipediaApi should timeout") {
    val ex = new RuntimeException
    val ob = Observable.just(1, 2, 3, 4) map { elt =>
      Thread.sleep(1000)
      elt
    }
    val recovered = ob.timedOut(2)

    val result = mutable.Buffer[Int]()
    val sub = recovered subscribe {
      result += _
    }
    assert(result.length <= 2)
  }
  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (n: Int) => Observable.just(0 to n : _*)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }

}
