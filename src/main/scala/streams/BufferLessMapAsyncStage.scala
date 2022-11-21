package streams

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, Supervision}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

case class BufferLessMapAsyncStage[In, Out](f: In => Future[Out]) extends GraphStage[FlowShape[In, Out]] {
  private val in = Inlet[In]("BufferLessMapAsyncStage.in")
  private val out = Outlet[Out]("BufferLessMapAsyncStage.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    // done: why do we need this decider: it is so that the one specified on the Flow element gets used.
    // done: why do we need this getAsyncCallback: see akka docs at: https://doc.akka.io/docs/akka/current/stream/stream-customize.html#using-asynchronous-side-channels
    // done: why do we need this invokeFutureCB? See answer to getAsyncCallback
    // done: how does tryPull work: requests an element on a port unless it has already been closed. Fails if called twice before an element has arrived.
    // done: how does hasBeenPulled work: indicates whether there is already a pending pull
    // todo: why did they have isCompleted within the futureCompleted's scope?

    private var inFlight = 0

    def tryPullSafe: Unit = {
      if (!hasBeenPulled(in)) tryPull(in)
    }

    def isCompleted: Boolean = isClosed(in) && inFlight == 0

    override def toString = s"BufferLessMapAsyncStage(f=$f)"

    val decider =
      inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

    def futureCompleted(result: Try[Out]): Unit = {

      inFlight -= 1

      result match {
        case Success(elem) if elem != null =>
          push(out, elem)
          // if we have no elements in flight and the port is closed:
          if (isCompleted) completeStage()
        case Success(_) =>
          if (isCompleted) completeStage()
          // If the element is null and there is no other pull in progress:
          else tryPullSafe
        case Failure(ex) =>
          // if inherited supervisionStrategy says STOP
          if (decider(ex) == Supervision.Stop) failStage(ex)
          // if we have no elements in flight and the port is closed:
          else if (isCompleted) completeStage()
          // If the supervision strategy is not Stop in case of a failure and there is no other pull in progress:
          else tryPullSafe
      }
    }

    private val futureCB = getAsyncCallback(futureCompleted)

    private val invokeFutureCB: Try[Out] => Unit = futureCB.invoke

    override def onPush(): Unit =
      try {
        val future = f(grab(in))
        inFlight += 1
        future.value match {
          case None => future.onComplete(invokeFutureCB)(scala.concurrent.ExecutionContext.parasitic)
          case Some(v) => futureCompleted(v)
        }
      } catch {
        case NonFatal(ex) => if (decider(ex) == Supervision.Stop) failStage(ex) else tryPullSafe
      }

    override def onUpstreamFinish(): Unit =
      if (inFlight == 0) completeStage()

    override def onPull(): Unit =
      if (isCompleted) completeStage()
      else tryPullSafe

    setHandlers(in, out, this)
  }
}
