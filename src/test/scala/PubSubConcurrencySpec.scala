import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.util.concurrent.{Executors, Future}
import scala.collection.mutable
import scala.concurrent.{Await, Promise, blocking}
import scala.concurrent.duration.{DurationDouble, DurationInt, FiniteDuration}
import scala.language.postfixOps

class PubSubConcurrencySpec extends TestKit(ActorSystem("BufferLessMapAsyncStageActorSystem"))
  with AnyWordSpecLike
  with should.Matchers
  with BeforeAndAfterAll
  with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(120 seconds)

  "BatchOrigin shaped stream" should {
    "not block with asyncFlow" in new PubSubConcurrencyFixture {
      fullySyncFlow.via(unwrapOption).via(flatMapStage).scan(0)((a, _) => a + 1).runWith(Sink.foreach {
        case i if i <= nMessages => println(s"processed element count ${i}")
        case i => throw new RuntimeException("Message reprocessing happened!")
      }).futureValue
    }

    "not block with fullySyncFlow" in new PubSubConcurrencyFixture {
      asyncFlow.via(unwrapOption).via(flatMapStage).scan(0)((a, _) => a + 1).runWith(Sink.foreach {
        case i if i <= nMessages => println(s"processed element count ${i}")
        case i => throw new RuntimeException("Message reprocessing happened!")
      }).futureValue
    }
  }
}

trait PubSubConcurrencyFixture extends LowThroughputPubSubProvider {
  val initDelay = 0 seconds
  val interval = 250 millis
  val tickValue = "tick"
  val syncTimeout = 20 seconds
  val nSubstreams = 5
  val ackExtensionInterval: FiniteDuration = (ackDeadline.toMillis * 0.6) millis

  val tick: Source[String, Cancellable] = Source.tick(initDelay, interval, tickValue)
  val unwrapOption: Flow[Option[Int], Int, NotUsed] = Flow[Option[Int]].mapConcat[Int](perhapsInt => perhapsInt.toList)

  val asyncFlow: Source[Option[Int], Cancellable] = tick.mapAsync(1)(_ => {
    val promise = Promise[Option[Int]]()
    Server.handlePull(promise)
    promise.future
  })

  val fullySyncFlow: Source[Option[Int], Cancellable] =
    tick.map(_ => {
      val promise = Promise[Option[Int]]()
      Server.handlePull(promise)
      var maybeInt: Option[Int] = None
      blocking {
        maybeInt = Await.result[Option[Int]](promise.future, syncTimeout)
      }
      maybeInt
    })

  val flatMapStage = Flow[Int].flatMapMerge(nSubstreams, item =>
    Source
      .single(item)
      .wireTap(ackExtenderSink)
      .async
      .map(v => {
        Server.handleAck(Promise(), v)
        v
      })
  )

  val ackExtenderSink = Flow[Int]
    .flatMapConcat(v => Source.tick(initDelay, ackExtensionInterval, v))
    .to(Sink.foreach(v => {
      Server.handleAckExtension(Promise[Int](), v)
    }))

}

trait LowThroughputPubSubProvider {
  lazy val nMessages: Int = 4
  lazy val ackDeadline: FiniteDuration = 1.5 seconds
  lazy val pullLatency: FiniteDuration = 500 millis
  lazy val ackExtensionLatency: FiniteDuration = 100 millis
  lazy val ackLatency: FiniteDuration = 100 millis

  case class ServerQueue(size: Int) {
    val waitingForPull: mutable.Queue[Int] = scala.collection.mutable.Queue() ++ (1 to nMessages)
    val waitingForAck = scala.collection.mutable.Map[Int, Instant]()
    var expiredAcks = 0
  }

  case object Server {
    val queueProvider = ServerQueue(nMessages)
    val waitingForPull = queueProvider.waitingForPull
    val waitingForAck = queueProvider.waitingForAck
    val pullThreadpool = Executors.newFixedThreadPool(4)
    val ackThreadPool = Executors.newFixedThreadPool(nMessages)

    def refreshBuffers(requestTimestamp: Instant) = {
      queueProvider.synchronized {
        (1 to nMessages).foreach(i => {
          waitingForAck.get(i).foreach(inst => {
            if (inst.isBefore(requestTimestamp)) {
              println(s"${i}: Expired ack extension for message.")
              queueProvider.expiredAcks += 1
              waitingForAck -= i
              waitingForPull += i
            } else {
              println("No expiration")
            }
          })
        })
      }
    }

    def ackItem(id: Int): Unit = {
      queueProvider.synchronized {
        if (waitingForAck.contains(id)) {
          waitingForAck -= id
          println(s"Successful ack for item ${id}")
          println(waitingForAck)
        } else {
          println(s"Failed ack for item ${id}")
        }
      }
    }


    def extendAck(id: Int): Unit = {
      queueProvider.synchronized {
        val requestTimestamp = Instant.now()
        waitingForAck.foreach {
          case (k, v) if k == id && v.isAfter(requestTimestamp) => waitingForAck.update(k, requestTimestamp.plusMillis(ackDeadline.toMillis))
        }

      }
    }

    def pullItem(): Option[Int] = {
      var maybeRemoved: Option[Int] = None
      queueProvider.synchronized {
        maybeRemoved = waitingForPull.headOption
        println(s"${maybeRemoved}: pull processed, item will be sent")
        if (!waitingForPull.isEmpty) {
          waitingForAck += waitingForPull.dequeue() -> Instant.now().plusMillis(ackDeadline.toMillis)
        }
      }
      maybeRemoved
    }

    def handlePull(prom: Promise[Option[Int]]) = {

      pullThreadpool.submit(() => {
        refreshBuffers(Instant.now())
        val maybeRemoved = pullItem()
        Thread.sleep(pullLatency.toMillis)
        prom.success(maybeRemoved)
      })
    }

    def handleAckExtension(prom: Promise[Int], id: Int): Future[prom.type] = {

      pullThreadpool.submit(() => {
        val requestTimestamp = Instant.now()
        refreshBuffers(requestTimestamp)
        extendAck(id)
        Thread.sleep(ackExtensionLatency.toMillis)
        prom.success(id)
      })
    }

    def handleAck(prom: Promise[Int], id: Int) = {

      pullThreadpool.submit(() => {
        refreshBuffers(Instant.now())
        ackItem(id)
        Thread.sleep(ackExtensionLatency.toMillis)
        prom.success(id)
      })
    }
  }
}
