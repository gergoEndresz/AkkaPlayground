import java.time.Instant
import java.util.concurrent.{Executors, Future, Semaphore, ThreadPoolExecutor}
import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration.{DurationDouble, DurationInt, FiniteDuration}
import scala.language.postfixOps

class PubSubConcurrencySpec {

}

trait PubSubConcurrencyFixture {
  val nMessages: Int = 4
  val ackDeadline: FiniteDuration = 5 seconds
  val pullLatency: FiniteDuration = 1.5 seconds
  val ackExtensionLatency: FiniteDuration = 100 millis
  val ackLatency: FiniteDuration = 100 millis

  case object Server {
    val messageQueue: mutable.Queue[Int] = scala.collection.mutable.Queue() ++ (1 to nMessages)
    val unAckedBuffer = scala.collection.mutable.Map[Int, Instant]()
    val threadPool = Executors.newFixedThreadPool(nMessages)
    val semaphore = new Semaphore(1)

    def refreshBuffers(requestTimestamp: Instant) = {
      (1 to nMessages).foreach(i => {
        unAckedBuffer.get(i).foreach(inst => {
          if (inst.isBefore(requestTimestamp)) {
            unAckedBuffer -= i
            messageQueue += i
          }
        })
      })
    }

    def handlePull(prom: Promise[Option[Int]]) = {
      semaphore.acquire()
      refreshBuffers(Instant.now())
      val maybeRemoved = messageQueue.headOption
      if (!messageQueue.isEmpty) {
        unAckedBuffer += messageQueue.dequeue() -> Instant.now().plusMillis(ackDeadline.toMillis)
      }
      semaphore.release()

      threadPool.submit(() => {
        threadPool.wait(pullLatency.toMillis)
        prom.success(maybeRemoved)
      })
    }

    def handleAckExtension(prom: Promise[Int], id: Int): Future[prom.type] = {
      semaphore.acquire()

      val requestTimestamp = Instant.now()
      refreshBuffers(requestTimestamp)

      unAckedBuffer.foreach {
        case (k, v) if k == id && v.isAfter(requestTimestamp) => unAckedBuffer.update(k, requestTimestamp.plusMillis(ackDeadline.toMillis))
      }

      semaphore.release()
      threadPool.submit(() => {
        threadPool.wait(ackExtensionLatency.toMillis)
        prom.success(id)
      })
    }

    def receiveAck(prom: Promise[Int], id: Int) = {
      semaphore.acquire()
      refreshBuffers(Instant.now())
      semaphore.release()

      threadPool.submit(() => {
        threadPool.wait(ackExtensionLatency.toMillis)
        prom.success(id)
      })
    }
  }
}
