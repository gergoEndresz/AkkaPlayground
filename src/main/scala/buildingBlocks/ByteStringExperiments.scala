package buildingBlocks

import akka.util.ByteString

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object ByteStringExperiments extends App
{
 val test = "test"
  println(ByteString(test).length)

  import java.nio.charset.StandardCharsets

  val rawString = "test"
  val bytes = rawString.getBytes(StandardCharsets.UTF_8)
  println(bytes.length)
  val utf8EncodedString = new String(bytes, StandardCharsets.UTF_8)

  assert(rawString == utf8EncodedString)

 println((0 seconds).length == 0)
}
