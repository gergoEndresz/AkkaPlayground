package streams

import akka.actor.ActorSystem
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink}
import akka.testkit.{TestKit, TestLatch}
import org.scalatest.{BeforeAndAfterAll}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike
import streams.MapAsyncSpecsFixture.TestException

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.control.{NoStackTrace}

class BufferLessMapAsyncStageSpec extends TestKit(ActorSystem("BufferLessMapAsyncStageActorSystem")) with AnyWordSpecLike with should.Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val executionContext = system.dispatcher

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(20 seconds)

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, 5 minutes, verifySystemShutdown = true)
  }

  // Todo: First test is only to demonstrate the problematic behaviour.
  // done: cross check tests with akka.stream.scaladsl.FlowMapAsyncUnorderedSpec
  // Todo: standardize test names
  // Todo: remove unnecessary tests
  // Todo: what is a test latch: it is an object (semaphore) that can signal execution of a background process
  // todo: write a test that simulates slow pubsub! Monitor pre-mapAsync stage by a testProbe to ensure that no messages are passed, but demand should be greater than 5!

  "mapAsync" should {
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

  "BufferLessMapAsync" should {
    "have NO additional elements inFlight if only one has been requested" in {
      val promise = Promise[Int]

      val testFlow = BufferLessMapAsyncStage[Int, Int] {
        i => {
          Thread.sleep(100)
          promise.success(i)
          Future(i)
        }
      }

      val publisher = Source(1 to 10)

      val subscriber = publisher.via(testFlow).runWith(TestSink[Int]())

      subscriber.request(1).expectNext(1).cancel().expectNoMessage()
    }

    // The following tests have been adopted from/inspired by akka.stream.scaladsl.FlowMapAsyncUnorderedSpec


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


    // Error scenarios - todo

    // todo probs not necessary
    "signal future failure" in {

      val latch = TestLatch(1)
      val c = TestSubscriber.manualProbe[Int]()


      val testFlow = BufferLessMapAsyncStage[Int, Int] { n =>
        Future {
          if (n == 1) throw TestException("err")
          else {
            Await.ready(latch, 10.seconds)
            n
          }
        }
      }

      Source(1 to 5)
        .via(testFlow)
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("err")
      latch.countDown()
    }

    // todo probs not necessary
    "signal future failure from upstream asap" in {

      val testFlow = BufferLessMapAsyncStage[Int, Int] {
        n =>
          if (n == 1) Future.failed(TestException("err"))
          else Future.successful(n)
      }


      val latch = TestLatch(1)
      val done = Source(1 to 5)
        .map { n =>
          if (n == 1) n
          else {
            // slow upstream should not block the error
            Await.ready(latch, 10.seconds)
            n
          }
        }.via(testFlow)
        .runWith(Sink.ignore)
      intercept[RuntimeException] {
        Await.result(done, remainingOrDefault)
      }.getMessage should be("err1")
      latch.countDown()
    }

    "signal error from BufferLessMapAsyncStage if thrown OUTSIDE the returned Future" in {

      val c = TestSubscriber.manualProbe[Int]()

      val testFlow = BufferLessMapAsyncStage[Int, Int](n =>
        if (n == 1) throw TestException("err")
        else {
          Future {
            Thread.sleep(100)
            n
          }
        })

      val subscriber = Source(1 to 5)
        .via(testFlow)
        .runWith(TestSink[Int]())

      val sub = c.expectSubscription()
      subscriber.requestNext(10).expectError().getMessage should be("err")
    }

    "signal error from BufferLessMapAsyncStage if thrown INSIDE the returned Future" in {

      val latch = TestLatch(1)
      val c = TestSubscriber.manualProbe[Int]()

      val testFlow = BufferLessMapAsyncStage[Int, Int](n =>

        Future {
          if (n == 1) throw TestException("err2")
          else {
            Await.ready(latch, 10.seconds)
            n
          }

        })

      Source(1 to 5)
        .via(testFlow)
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("err2")
      latch.countDown()
    }

    "resume when error is thrown INSIDE the Future if resumingDecider is specified" in {
      val testFlow = BufferLessMapAsyncStage[Int, Int](n =>
        Future {
          if (n == 3) throw TestException("err3")
          else n
        })

      Source(1 to 5)
        .via(testFlow)
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(TestSink[Int]())
        .request(10)
        .expectNextUnordered(1, 2, 4, 5)
        .expectComplete()
    }

    "resume when error is thrown OUTSIDE the Future if resumingDecider is specified" in {

      val testFlow = BufferLessMapAsyncStage[Int, Int] { n =>
        if (n == 3) throw TestException("err4")
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

    "finish after future failure if resumingDecider is specified" in {
      val testFlow = BufferLessMapAsyncStage[Int, Int] { n =>
        Future {
          if (n == 2) throw TestException("err5")
          else n
        }
      }

      Source(1 to 3)
        .via(testFlow)
        .withAttributes(supervisionStrategy(resumingDecider))
        .grouped(10)
        .runWith(Sink.head).futureValue should be(Seq(1, 3))
    }

    "resume after multiple failures if resumingDecider is specified" in {
      val futures: List[Future[String]] = List(
        Future.failed(TestException("failure1")),
        Future.failed(TestException("failure2")),
        Future.failed(TestException("failure3")),
        Future.failed(TestException("failure4")),
        Future.failed(TestException("failure5")),
        Future.successful("happy!"))

      Await.result(
        Source(futures)
          .via(BufferLessMapAsyncStage(identity))
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(Sink.head),
        3.seconds) should ===("happy!")
    }

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

      val result = Source(0 to 200).via(testFlow).runWith(Sink.seq)

      result.futureValue shouldBe empty
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
}

object MapAsyncSpecsFixture {
  case class TestException(message: String) extends RuntimeException(message) with NoStackTrace
}


