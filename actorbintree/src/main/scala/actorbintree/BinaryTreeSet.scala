/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation =>
      log.debug(s"Emit $op to $root")
      root ! op
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      log.debug(s"Enter GC Mode with $newRoot as newRoot")
      context.become(garbageCollecting(newRoot))
  }

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation =>
      log.debug(s"Add operation $op to pending")
      pendingQueue = pendingQueue enqueue op
    case CopyFinished =>
      log.debug("Copy is done")
      root = newRoot
      for (op <- pendingQueue) {
        log.debug(s"Emit $op to $root")
        root ! op
      }
      pendingQueue = Queue.empty[Operation]
      log.debug("Enter Normal Mode")
      context.become(normal)
    case GC =>
      // just ignore it
      ()
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor
    with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved
  var reportTo: Option[ActorRef] = None

  // optional
  def receive = normal

  private def insertAtChild(p: Position, insert: Insert) {
    if (subtrees contains p) {
      subtrees(p) ! insert
    } else {
      val e = insert.elem
      subtrees = subtrees + (p ->
        context.actorOf(props(insert.elem, false), s"node$e"))
      insert.requester ! OperationFinished(insert.id)
    }
  }

  private def removeAtChild(p: Position, remove: Remove) {
    if (subtrees contains p) {
      subtrees(p) ! remove
    } else {
      remove.requester ! OperationFinished(remove.id)
    }
  }

  private def containsAtChild(p: Position, contains: Contains) {
    if (subtrees contains p) {
      subtrees(p) ! contains
    } else {
      contains.requester ! ContainsResult(contains.id, false)
    }
  }

  private def copyAtChild(p: Position, copy: CopyTo) {
    if (subtrees contains p) {
      subtrees(p) ! copy
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, elem) =>
      if (elem == this.elem) {
        if (removed) removed = false
        requester ! OperationFinished(id)
      } else if (elem < this.elem) {
        insertAtChild(Left, Insert(requester, id, elem))
      } else {
        insertAtChild(Right, Insert(requester, id, elem))
      }
    case Remove(requester, id, elem) =>
      if (elem == this.elem) {
        if (!removed) removed = true
        requester ! OperationFinished(id)
      } else if (elem < this.elem) {
        removeAtChild(Left, Remove(requester, id, elem))
      } else {
        removeAtChild(Right, Remove(requester, id, elem))
      }
    case Contains(requester, id, elem) =>
      if (elem == this.elem) {
        requester ! ContainsResult(id, !removed)
      } else if (elem < this.elem) {
        containsAtChild(Left, Contains(requester, id, elem))
      } else {
        containsAtChild(Right, Contains(requester, id, elem))
      }
    case CopyTo(actorRef) =>
      if (!removed)
        actorRef ! Insert(self, 0, elem)
      copyAtChild(Left, CopyTo(actorRef))
      copyAtChild(Right, CopyTo(actorRef))
      reportTo = Some(sender)
      val expected = subtrees.values.toSet
      if (expected.isEmpty && removed) {
        stop
      } else {
        log.debug(s"Enter Copying Mode: $expected")
        context.become(copying(expected, removed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished if (insertConfirmed) =>
      log.debug(s"$sender finished")
      if (expected.dropWhile(_ == sender).isEmpty) {
          stop
      } else {
        val e = expected - sender
        log.debug(s"Enter Copying Mode: $e, $insertConfirmed")
        context.become(copying(e, insertConfirmed))
      }
    case CopyFinished if (!insertConfirmed) =>
      log.debug(s"$sender finished")
      val e = expected - sender
      log.debug(s"Enter Copying Mode: $e, $insertConfirmed")
      context.become(copying(e, insertConfirmed))
    case OperationFinished(_) =>
      log.debug("Insertion finished.")
      if (expected.isEmpty) {
        stop
      } else {
        log.debug(s"Enter Copying Mode: $expected, true")
        context.become(copying(expected, true))
      }
  }

  def stop() {
    log.debug("successfully finished copying, shutting down")
    reportTo match {
      case Some(a) =>
        a ! CopyFinished
      case _ => ()
    }
    context.stop(self)
  }
}
