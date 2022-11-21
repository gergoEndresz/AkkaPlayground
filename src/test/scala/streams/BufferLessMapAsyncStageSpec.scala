package streams

import akka.actor.ActorSystem
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink}
import akka.testkit.{TestKit, TestLatch, TestProbe}
import org.scalatest.{BeforeAndAfterAll}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike
import streams.MapAsyncSpecsFixture.TE

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.control.{NoStackTrace}

class BufferLessMapAsyncStageSpec extends TestKit(ActorSystem("TestActorSystem")) with AnyWordSpecLike with should.Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val executionContext = system.dispatcher

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(20 seconds)

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, 5 minutes, verifySystemShutdown = true)
  }

  // Todo: This is only to demonstrate the problematic behaviour.
  // Todo: cross check tests with akka.stream.scaladsl.FlowMapAsyncUnorderedSpec
  // Todo: standardize test names
  // Todo: remove unnecessary tests
  // Todo: what is a test latch?

  "mapAsync" should {
    "have an additional element inFlight if one has been requested" in {

      val promise = Promise[Int]

      val asyncFlow = Flow[Int].mapAsync(1) {
        i => {
          promise.success(i)
          Future(i)
        }
      }

      val testSource = Source(1 to 10)
      val testSink = TestSink.probe[Int]
      val subscriber = testSource.via(asyncFlow).toMat(testSink)(Keep.right).run()

      val throwable = subscriber.request(1).expectNext(1).cancel().expectError()
      throwable.getMessage shouldBe "Promise already completed."
    }
  }

  "BufferLessMapAsync" should {
    "have NO additional elements inFlight if only one has been requested" in {
      val promise = Promise[Int]

      val testSubject = BufferLessMapAsyncStage[Int, Int] {
        i => {
          promise.success(i)
          Future(i)
        }
      }

      val testSource = Source(1 to 10)
      val testSink = TestSink.probe[Int]
      val subscriber = testSource.via(testSubject).toMat(testSink)(Keep.right).run()

      subscriber.request(1).expectNext(1).cancel().expectNoMessage()
    }

    // The following tests have been adopted from/inspired by akka.stream.scaladsl.FlowMapAsyncUnorderedSpec

    "complete without requiring further demand (parallelism = 1)" in {
      val testSubject = BufferLessMapAsyncStage[Int, Int] { v =>
        Future {
          Thread.sleep(20);
          v
        }
      }

      Source
        .single(1)
        .via(testSubject)
        .runWith(TestSink[Int]())
        .requestNext(1)
        .expectComplete()
    }

    "complete without requiring further demand with already completed future (parallelism = 1)" in {

      val testSubject = BufferLessMapAsyncStage[Int, Int] {
        v => Future.successful(v)
      }

      Source
        .single(1)
        .via(testSubject)
        .runWith(TestSink[Int]())
        .requestNext(1)
        .expectComplete()
    }

    "produce future elements in the order they are pulled from the source" in {
      //a.k.a => "produce future elements in the order they are ready"
    }


    "complete without requiring further demand if there is more than one element" in {

      val testSubject = BufferLessMapAsyncStage[Int, Int] { v =>
        Future {
          Thread.sleep(20);
          v
        }
      }

      val probe =
        Source(1 :: 2 :: Nil).via(testSubject).runWith(TestSink[Int]())

      probe.request(2).expectNextN(2)
      probe.expectComplete()
    }

    "complete without requiring further demand with already completed future if there are more than one elements" in {

      val testSubject = BufferLessMapAsyncStage[Int, Int] { v =>
        Future.successful(v)
      }

      val probe = Source(1 :: 2 :: Nil).via(testSubject).runWith(TestSink[Int]())

      probe.request(2).expectNextN(2)
      probe.expectComplete()
    }

    // todo probs not necessary
    "signal future failure" in {

      val latch = TestLatch(1)
      val c = TestSubscriber.manualProbe[Int]()


      val testSubject = BufferLessMapAsyncStage[Int, Int] { n =>
        Future {
          if (n == 1) throw new RuntimeException("err1") with NoStackTrace
          else {
            Await.ready(latch, 10.seconds)
            n
          }
        }
      }

      Source(1 to 5)
        .via(testSubject)
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("err1")
      latch.countDown()
    }

    // todo probs not necessary
    "signal future failure from upstream asap" in {

      val testSubject = BufferLessMapAsyncStage[Int, Int] {
        n =>
          if (n == 1) Future.failed(new RuntimeException("err1") with NoStackTrace)
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
        }.via(testSubject)
        .runWith(Sink.ignore)
      intercept[RuntimeException] {
        Await.result(done, remainingOrDefault)
      }.getMessage should be("err1")
      latch.countDown()
    }

    "signal error from BufferLessMapAsyncStage" in {
      val latch = TestLatch(1)
      val c = TestSubscriber.manualProbe[Int]()

      val testSubject = BufferLessMapAsyncStage[Int, Int](n =>
        if (n == 1) throw new RuntimeException("err2") with NoStackTrace
        else {
          Future {
            Await.ready(latch, 10.seconds)
            n
          }
        })

      Source(1 to 5)
        .via(testSubject)
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("err2")
      latch.countDown()
    }

    "resume after future failure if resumingDecider is specified" in {
      val testSubject = BufferLessMapAsyncStage[Int, Int](n =>
        Future {
          if (n == 3) throw new RuntimeException("err3") with NoStackTrace
          else n
        })

      Source(1 to 5)
        .via(testSubject)
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(TestSink[Int]())
        .request(10)
        .expectNextUnordered(1, 2, 4, 5)
        .expectComplete()
    }

    "resume after multiple failures if resumingDecider is specified" in {
      val futures: List[Future[String]] = List(
        Future.failed(TE("failure1")),
        Future.failed(TE("failure2")),
        Future.failed(TE("failure3")),
        Future.failed(TE("failure4")),
        Future.failed(TE("failure5")),
        Future.successful("happy!"))

      Await.result(
        Source(futures)
          .via(BufferLessMapAsyncStage(identity))
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(Sink.head),
        3.seconds) should ===("happy!")
    }

    "finish after future failure if resumingDecider is specified" in {
      val testSubject = BufferLessMapAsyncStage[Int, Int] { n =>
        Future {
          if (n == 2) throw new RuntimeException("err3b") with NoStackTrace
          else n
        }
      }

      Source(1 to 3)
        .via(testSubject)
        .withAttributes(supervisionStrategy(resumingDecider))
        .grouped(10)
        .runWith(Sink.head).futureValue should be(Seq(1, 3))
    }

    "resume when error is thrown outside the Future if resumingDecider is specified" in {

      val testSubject = BufferLessMapAsyncStage[Int, Int] { n =>
        if (n == 3) throw new RuntimeException("err4") with NoStackTrace
        else Future(n)
      }

      Source(1 to 5)
        .via(testSubject)
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

    "ignore element when future is completed with null" in {

      val testSubject = BufferLessMapAsyncStage[Int, String] {
        case 2 => Future.successful(null)
        case x => Future.successful(x.toString)
      }

      val result = Source(List(1, 2, 3)).via(testSubject).runWith(Sink.seq)

      result.futureValue should contain.only("1", "3")
    }

    "continue emitting after a sequence of nulls" in {
      val testSubject = BufferLessMapAsyncStage[Int, String] { value =>
        if (value == 0 || value >= 100) Future.successful(value.toString)
        else Future.successful(null)
      }

      val result = Source(0 to 102).via(testSubject).runWith(Sink.seq)

      result.futureValue should contain.only("0", "100", "101", "102")
    }

    "complete without emitting any element after a sequence of nulls only" in {
      val testSubject = BufferLessMapAsyncStage[Int, String] { _ =>
        Future.successful(null)
      }

      val result = Source(0 to 200).via(testSubject).runWith(Sink.seq)

      result.futureValue shouldBe empty
    }

    "complete stage if future with null result is completed last" in {

      val latch = TestLatch(2)

      val testSubject = BufferLessMapAsyncStage[Int, String] {
        case 3 =>
          Future {
            //Await.ready(latch, 10.seconds)
            null
          }
        case x =>
          latch.countDown()
          Future.successful(x.toString)
      }


      val result = Source(List(1, 2, 3)).via(testSubject).runWith(Sink.seq)

      result.futureValue should contain.only("1", "2")
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
  case class TE(message: String) extends RuntimeException(message) with NoStackTrace
}


