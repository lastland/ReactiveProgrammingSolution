package suggestions



import scala.collection._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import scala.swing.event.Event
import scala.swing.Reactions.Reaction
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SwingApiTest extends FunSuite {

  object swingApi extends SwingApi {
    class ValueChanged(val textField: TextField) extends Event

    object ValueChanged {
      def unapply(x: Event) = x match {
        case vc: ValueChanged => Some(vc.textField)
        case _ => None
      }
    }

    class ButtonClicked(val source: Button) extends Event

    object ButtonClicked {
      def unapply(x: Event) = x match {
        case bc: ButtonClicked => Some(bc.source)
        case _ => None
      }
    }

    class Component {
      private val subscriptions = mutable.Set[Reaction]()
      def subscribe(r: Reaction) {
        subscriptions add r
      }
      def unsubscribe(r: Reaction) {
        subscriptions remove r
      }
      def publish(e: Event) {
        for (r <- subscriptions) r(e)
      }
    }

    class TextField extends Component {
      private var _text = ""
      def text = _text
      def text_=(t: String) {
        _text = t
        publish(new ValueChanged(this))
      }
    }

    class Button extends Component {
      def click() {
        publish(new ButtonClicked(this))
      }
    }
  }

  import swingApi._

  test("SwingApi should emit text field values to the observable") {
    val textField = new swingApi.TextField
    val values = textField.textValues

    val observed = mutable.Buffer[String]()
    val sub = values subscribe {
      observed += _
    }
    val observed2 = mutable.Buffer[String]()
    val sub2 = values subscribe {
      observed2 += _
    }

    // write some text now
    textField.text = "T"
    textField.text = "Tu"
    textField.text = "Tur"
    textField.text = "Turi"
    textField.text = "Turin"
    textField.text = "Turing"

    sub.unsubscribe

    textField.text = "Turing!"

    sub2.unsubscribe

    assert(observed == Seq("T", "Tu", "Tur", "Turi", "Turin", "Turing"), observed)
    assert(observed2 == Seq("T", "Tu", "Tur", "Turi", "Turin", "Turing", "Turing!"), observed2)
  }

  test("SwingApi should emit buttons to the observable") {
    val button = new swingApi.Button
    val clicks = button.clicks

    val observed = mutable.Buffer[swingApi.Button]()
    val sub = clicks subscribe {
      observed += _
    }
    val observed2 = mutable.Buffer[swingApi.Button]()
    val sub2 = clicks subscribe {
      observed2 += _
    }

    button.click
    sub.unsubscribe

    button.click
    sub2.unsubscribe

    assert(observed == Seq(button), observed)
    assert(observed2 == Seq(button, button), observed2)
  }


}
