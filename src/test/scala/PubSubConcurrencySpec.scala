import akka.NotUsed
import akka.actor.{ActorSystem}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout => PatienceTimeout}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.time.{Seconds, Span}
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

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(120, Seconds)), interval = scaled(Span(1, Seconds)))

  "BatchOrigin shaped stream" should {
    "not be blocked by Pub/Sub pull" when {


      "used with fullySyncFlow" in new PubSubConcurrencyFixture {
        val sys = ActorSystem("Test1")
        val materializer = Materializer(sys)
        override lazy val testId = "test-1"

        private val (switch: UniqueKillSwitch, testSink) = fullySyncFlow
          .via(unwrapOption)
          .via(flatMapStage)
          .scan(0)((a, _) => a + 1)
          .toMat(probe)(Keep.both)
          .run()(materializer)

        testSink.request(10)

        eventually(PatienceTimeout(10 seconds), Interval(1 second)) {
          Server.queueProvider.expiredAcks should be > 0
        }

        switch.shutdown()
        Server.shutdown()
      }

      "used with asyncFlow" in new PubSubConcurrencyFixture {
        implicit val sys = ActorSystem("Test2")
        val materializer = Materializer(sys)
        override lazy val testId = "test-2"
        private val (switch, testSink) = asyncFlow.via(unwrapOption).via(flatMapStage).scan(0)((a, _) => a + 1).toMat(probe)(Keep.both).run()(materializer)

        testSink.request(10).expectNext(0, 1, 2).expectNoMessage(3 seconds)
        Server.queueProvider.expiredAcks shouldBe (0)

        switch.shutdown()
        Server.shutdown()
      }
    }

  }
}

abstract class PubSubConcurrencyFixture(implicit val system: ActorSystem) extends LowThroughputPubSubProvider {
  override val nMessages: Int = 2

  val killSwitch = KillSwitches.single[String]

  implicit val timeout = Timeout(10 seconds)
  val probe = TestSink[Int]()

  val tickValue = "tick"
  val nSubstreams = 3

  val tick: Source[String, UniqueKillSwitch] = Source.tick(pullInitDelay, pullInterval, tickValue).viaMat(KillSwitches.single[String])(Keep.right)
  val unwrapOption: Flow[Option[Int], Int, NotUsed] = Flow[Option[Int]].mapConcat[Int](perhapsInt => perhapsInt.toList)

  val asyncFlow: Source[Option[Int], UniqueKillSwitch] = tick.mapAsync(1)(_ => {
    val promise = Promise[Option[Int]]()
    Server.handlePull(promise)
    promise.future
  })

  val fullySyncFlow: Source[Option[Int], UniqueKillSwitch] =
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
    .flatMapConcat(v => Source.tick(pullInitDelay, ackExtensionInterval, v))
    .to(Sink.foreach(v => {
      Server.handleAckExtension(Promise[Int](), v)
    }))

}

trait LowThroughputPubSubProvider {


  val nMessages: Int

  val pullInitDelay = 0 seconds
  val pullInterval = 500 millis
  val ackExtensionInterval: FiniteDuration = (ackDeadline.toMillis * 0.6) millis
  lazy val ackDeadline: FiniteDuration = 2 seconds
  val syncTimeout = 20 seconds

  lazy val pullLatency: FiniteDuration = 1 second
  lazy val ackExtensionLatency: FiniteDuration = 100 millis
  lazy val ackLatency: FiniteDuration = 100 millis

  case class ServerQueue(size: Int) {
    val waitingForPull: mutable.Queue[Int] = scala.collection.mutable.Queue() ++ (1 to nMessages)
    val waitingForAck = scala.collection.mutable.Map[Int, Instant]()
    var expiredAcks = 0
  }

  lazy val testId = "test-1"

  case object Server {
    val queueProvider = ServerQueue(nMessages)
    val waitingForPull = queueProvider.waitingForPull
    val waitingForAck = queueProvider.waitingForAck
    val threadPool = Executors.newFixedThreadPool(nMessages * 3)

    def shutdown(): Unit = {
      threadPool.shutdown()
    }

    def refreshBuffers(requestTimestamp: Instant) = {
      queueProvider.synchronized {
        (1 to nMessages).foreach(i => {
          waitingForAck.get(i).foreach(inst => {
            if (inst.isBefore(requestTimestamp)) {
              println(s"${testId}: Expired ack extension for message: ${i}")
              queueProvider.expiredAcks += 1
              waitingForAck -= i
              waitingForPull += i
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
        println(s"pull processed with item: ${maybeRemoved}")
        if (!waitingForPull.isEmpty) {
          waitingForAck += waitingForPull.dequeue() -> Instant.now().plusMillis(ackDeadline.toMillis)
        }
      }
      maybeRemoved
    }

    def handlePull(prom: Promise[Option[Int]]) = {

      threadPool.submit(() => {
        refreshBuffers(Instant.now())
        val maybeRemoved = pullItem()
        Thread.sleep(pullLatency.toMillis)
        prom.success(maybeRemoved)
      })
    }

    def handleAckExtension(prom: Promise[Int], id: Int): Future[prom.type] = {

      threadPool.submit(() => {
        val requestTimestamp = Instant.now()
        refreshBuffers(requestTimestamp)
        extendAck(id)
        Thread.sleep(ackExtensionLatency.toMillis)
        prom.success(id)
      })
    }

    def handleAck(prom: Promise[Int], id: Int) = {

      threadPool.submit(() => {
        refreshBuffers(Instant.now())
        ackItem(id)
        Thread.sleep(ackExtensionLatency.toMillis)
        prom.success(id)
      })
    }
  }
}
