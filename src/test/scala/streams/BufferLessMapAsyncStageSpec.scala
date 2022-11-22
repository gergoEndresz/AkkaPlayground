package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{TestKit, TestLatch, TestProbe}
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike
import streams.MapAsyncSpecsFixture.{TestException, TestExceptionMessage}

import java.util.concurrent.{Executors, ThreadPoolExecutor}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.control.NoStackTrace

class BufferLessMapAsyncStageSpec extends TestKit(ActorSystem("BufferLessMapAsyncStageActorSystem"))
  with AnyWordSpecLike
  with should.Matchers
  with BeforeAndAfterAll
  with ScalaFutures {

  implicit val executionContext = system.dispatcher

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(20 seconds)

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, 5 minutes, verifySystemShutdown = true)
  }

  // done: cross check tests with akka.stream.scaladsl.FlowMapAsyncUnorderedSpec
  // done: standardize test names
  // done: remove unnecessary tests
  // done: what is a test latch: it is an object (semaphore) that can signal execution of a background process
  // todo: write a test that simulates slow Pub/Sub! Monitor pre-mapAsync stage by a testProbe to ensure that no messages are passed, but demand should be greater than 5!
  // todo: double check error tests

  "mapAsync" should {
    // Todo: First test is only to demonstrate the problematic behaviour.
    "have an additional element inFlight if one has been requested" in {

      val promise = Promise[Int]

      val asyncFlow = Flow[Int].mapAsync(1) {
        i => {
          promise.success(i)
          Future(i)
        }
      }

      val publisher = Source(1 to 10)
      val subscriber = publisher.via(asyncFlow).toMat(TestSink[Int]())(Keep.right).run()

      val throwable = subscriber.request(1).expectNext(1).cancel().expectError()
      throwable.getMessage shouldBe "Promise already completed."
    }
  }

  "BufferLessMapAsync" when {
    "everything goes well" should {
      "have NO additional elements inFlight if only one has been requested" in {
        // BG for this test:
        // The problem with mapAsync(1) in our Pub/Sub client when using asyncFlow is that it
        // will have an additional message in-flight even if we have signalled demand for only one.
        // This can be problematic. Consider the following scneario:
        // 1. We have a Pub/Sub topic with an ackDeadline duration of 10s.
        // 2. We pull one message based on downstream demand. Then the mapAsync stage will pull another one (as a performance optimisation).
        // 3. Downstream takes 15s to process the first message, the ackDeadline expires for the second in-flight message after 10 seconds.
        // 4. Downstream starts processing and ackExtending the second message, but that is already too late.
        // 5. Then, Pub/Sub will re-deliver the second message entailing a different ackExtension and ack ID.
        // 6. Our stream will interpret the re-delivered message as a new one since it has no way of telling whether that message is actually currently being processed downstream.

        val promise = Promise[Int]

        val testFlow = BufferLessMapAsyncStage[Int, Int] {
          i => {
            Thread.sleep(100)
            promise.success(i)
            Future(i)
          }
        }

        val publisher = Source(1 to 10)

        val subscriber = publisher
          .via(testFlow).runWith(TestSink[Int]())

        subscriber.request(1).expectNext(1).cancel().expectNoMessage()
      }

      "waiting on external process to finish should not block downstream processing with async boundary" in {
        // BG for this test:
        // The second problem with out Pub/Sub client is that it Awaits the response from Pub/Sub if it is configured to run with "fullySyncFlow".
        // The await call is a blocking call stopping every downstream processing while it gets fulfilled
        // which coming from a low-throughput Pub/Sub topic can take up to 5 seconds..

        // We do not have such Await call in the 'asyncFlow' option, however, it was thought that in order
        // to ensure that the alternative implementation of mapAsync did not introduce such blocking processing stage,
        // this test was written.

        // Should probably be removed.

        // Rationale behind the test:
        // Upstream returns a future completed by downstream.
        // Downstream completes the future once upstream has entered into the processing stage of the 2nd message.
        // If BufferLessMapAsyncStage was a blocking stage, downstream would never be able to complete the future,
        // since it only gets to do so once the second message entered BufferLessMapAsyncStage.


        val promise = Promise[Int]
        val latch = TestLatch(1)

        val releasesLatchIfSecondElement = (i: Int) => {
          if (i == 2) {
            // This is just to ensure that downstream won't complete the future as long as the second message
            // has not entered the first stage.
            // Once that has happened, we complete the future downstream.
            // Meaning that downstream has managed to make progress independently from upstream.
            latch.countDown()
            promise.future
          }
          Future.successful(i)
        }

        val waitsForLatchIfFirstElement = (i: Int) => {
          // we only complete the future once upstream has released the lock on the latch
          if (i == 1) {
            Await.ready(latch, 10 seconds)
            promise.success(2)
          }
          i
        }

        val testFlow = BufferLessMapAsyncStage[Int, Int](releasesLatchIfSecondElement)

        val publisher = Source(1 to 10)

        val subscriber = publisher.via(testFlow).async.map(waitsForLatchIfFirstElement)
          .runWith(TestSink[Int]())

        subscriber.request(2).expectNext(1, 2).cancel().expectNoMessage()
      }

      "produce future elements preserving their upstream order" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int] { v =>
          Future {
            if (v == 2) Thread.sleep(500);
            v
          }
        }
        val subscriber =
          Source(1 :: 2 :: 3 :: Nil).via(testFlow).runWith(TestSink[Int]())

        subscriber
          .request(3)
          .expectNext(1, 2, 3)
          .expectComplete()

      }

      // The following tests have been adopted from/inspired by akka.stream.scaladsl.FlowMapAsyncUnorderedSpec

      "complete without requiring further demand with incomplete future" in {
        val testFlow = BufferLessMapAsyncStage[Int, Int] { v =>
          Future {
            Thread.sleep(200);
            v
          }
        }

        val subscriber = Source.single(1).via(testFlow).runWith(TestSink[Int]())

        subscriber
          .requestNext(1)
          .expectComplete()
      }

      "complete without requiring further demand with already completed future" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int] {
          v => Future.successful(v)
        }

        val subscriber = Source
          .single(1)
          .via(testFlow)
          .runWith(TestSink[Int]())

        subscriber
          .requestNext(1)
          .expectComplete()
      }

      "complete without requiring further demand if there is more than one element" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int] { v =>
          Future {
            Thread.sleep(20);
            v
          }
        }

        val subscriber = Source(1 :: 2 :: Nil)
          .via(testFlow)
          .runWith(TestSink[Int]())

        subscriber.request(2)
          .expectNext(1, 2)
          .expectComplete()
      }

      "complete without requiring further demand with already completed future if there is more than one elements" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int] { v =>
          Future.successful(v)
        }

        val subscriber = Source(1 :: 2 :: Nil)
          .via(testFlow)
          .runWith(TestSink[Int]())

        subscriber
          .request(2)
          .expectNext(1, 2)
          .expectComplete()
      }

      "handle cancel properly" in {
        val pub = TestPublisher.manualProbe[Int]()
        val sub = TestSubscriber.manualProbe[Int]()

        Source.fromPublisher(pub).via(BufferLessMapAsyncStage(Future.successful)).runWith(Sink.fromSubscriber(sub))

        val upstream = pub.expectSubscription()
        upstream.expectRequest()

        sub.expectSubscription().cancel()

        upstream.expectCancellation()
      }
    }

    "encountering errors" should {

      "signal such if thrown OUTSIDE the returned Future" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int](n =>
          if (n == 1) throw TestException()
          else {
            Future.successful(n)
          })

        val subscriber = Source(1 to 5)
          .via(testFlow)
          .runWith(TestSink[Int]())

        subscriber
          .request(10)
          .expectError()
          .getMessage should be(TestExceptionMessage)
      }

      "signal such if thrown INSIDE the returned Future" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int](n =>
          Future {
            if (n == 1) throw TestException(TestExceptionMessage)
            else {
              n
            }
          })

        val subscriber = Source(1 to 5)
          .via(testFlow)
          .runWith(TestSink[Int]())

        subscriber
          .request(10)
          .expectError()
          .getMessage should be(TestExceptionMessage)
      }
    }

    "resumingDecider is specified" should {
      "resume when error is thrown INSIDE the Future if resumingDecider" in {
        val testFlow = BufferLessMapAsyncStage[Int, Int](n =>
          Future {
            if (n == 3) throw TestException()
            else n
          })

        Source(1 to 5)
          .via(testFlow)
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(TestSink[Int]())
          .request(10)
          .expectNext(1, 2, 4, 5)
          .expectComplete()
      }

      "resume after multiple failures if resumingDecider" in {
        val futures: List[Future[String]] = List(
          Future.failed(TestException("failure1")),
          Future.failed(TestException("failure2")),
          Future.failed(TestException("failure3")),
          Future.failed(TestException("failure4")),
          Future.failed(TestException("failure5")),
          Future.successful("happy!"))

        Source(futures)
          .via(BufferLessMapAsyncStage(identity))
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(Sink.head).futureValue should ===("happy!")
      }

      "finish after future failure" in {
        val testFlow = BufferLessMapAsyncStage[Int, Int] { n =>
          Future {
            if (n == 2) throw TestException()
            else n
          }
        }

        Source(1 to 3)
          .via(testFlow)
          .withAttributes(supervisionStrategy(resumingDecider))
          .grouped(10)
          .runWith(Sink.head).futureValue should be(Seq(1, 3))
      }

      "resume when error is thrown OUTSIDE the Future" in {

        val testFlow = BufferLessMapAsyncStage[Int, Int] { n =>
          if (n == 3) throw TestException()
          else Future(n)
        }

        Source(1 to 5)
          .via(testFlow)
          .withAttributes(supervisionStrategy(resumingDecider))
          .map(v => {
            println(v);
            v
          })
          .runWith(TestSink[Int]())
          .request(10)
          .expectNext(1, 2, 4, 5)
          .expectComplete()
      }
    }

    "encountering nulls" should {
      "ignore element when future is completed with null" in {

        val testFlow = BufferLessMapAsyncStage[Int, String] {
          case 2 => Future.successful(null)
          case x => Future.successful(x.toString)
        }

        val result = Source(List(1, 2, 3)).via(testFlow).runWith(Sink.seq)

        result.futureValue should contain.only("1", "3")
      }

      "continue emitting after a sequence of nulls" in {
        val testFlow = BufferLessMapAsyncStage[Int, String] { value =>
          if (value == 0 || value >= 100) Future.successful(value.toString)
          else Future.successful(null)
        }

        val result = Source(0 to 102).via(testFlow).runWith(Sink.seq)

        result.futureValue should contain.only("0", "100", "101", "102")
      }

      "complete without emitting any element after a sequence of nulls only" in {
        val testFlow = BufferLessMapAsyncStage[Int, String] { _ =>
          Future.successful(null)
        }
        val value1: Source[String, NotUsed] = Source(0 to 200).via(testFlow)
        val result = Source(0 to 200).via(testFlow).runWith(Sink.seq)

        result.futureValue shouldBe empty
      }
    }
  }
}

object MapAsyncSpecsFixture {
  val TestExceptionMessage = "error"

  case class TestException(message: String = TestExceptionMessage) extends RuntimeException(message) with NoStackTrace
}


