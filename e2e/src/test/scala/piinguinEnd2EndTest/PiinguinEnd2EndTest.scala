/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package piinguinEnd2EndTest

// Scala
import scala.collection.mutable
import scala.concurrent.forkjoin.ForkJoinPool

// Java
import java.net.{InetSocketAddress, Socket}

// Scalatest
import org.scalatest.{BeforeAndAfterAll, CancelAfterFailure, Ignore, Tag, WordSpec}
import org.scalatest.Matchers._

// DynamoDB
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException

// Scala
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Right, Success, Try}

// Server main
import com.snowplowanalytics.piinguin.server.Main

// FS2
import fs2.async.mutable.Queue
import fs2.async
import fs2._

// Cats
import cats.effect.IO

// Client
import com.snowplowanalytics.piinguin.client.PiinguinClient

// Server
import com.snowplowanalytics.piinguin.server.PiinguinServer
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin.ReadPiiRecordRequest.LawfulBasisForProcessing
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin.PiiRecord

// From servers test
import com.snowplowanalytics.piinguin.server.clients.{DynamoDBTestClient, DynamoDBTestUtils}
import com.snowplowanalytics.piinguin.server.TestUtils

class PiinguinEnd2EndTest extends WordSpec with CancelAfterFailure with BeforeAndAfterAll {
  object WhenDynamodDBAvailable extends Tag(if (TestUtils.dynamoDbAvailable) "" else classOf[Ignore].getName)
  import ExecutionContext.Implicits.global
  val TEST_TABLE_NAME = "stuff"
  val SERVER_HOST     = "localhost"
  val SERVER_PORT     = 8080
  "Piinguin server" can {
    "start" should {
      "be possible to create a record" taggedAs WhenDynamodDBAvailable in {
        val piinguinClient = new PiinguinClient(SERVER_HOST, SERVER_PORT)
        val result         = Await.result(piinguinClient.createPiiRecord("1234", "567"), 10 seconds)
        result should be(Right("OK"))
      }
      "be possible to create records using fs2 streaming" taggedAs WhenDynamodDBAvailable in {
        val piinguinClient = new PiinguinClient(SERVER_HOST, SERVER_PORT)
        val requestQueue   = Stream.eval(Queue.unbounded[IO, (String, String)])
        val resultQueue    = Stream.eval(Queue.unbounded[IO, Either[String, String]])
        val r: Stream[IO, List[Either[String, String]]] = for {
          reqq <- requestQueue
          resq <- resultQueue
          _ = async.unsafeRunAsync(piinguinClient.createRecordsStreamingIO(maxQueued = 2, reqq, resq))(_ => IO.unit)
          _ = List(("1236", "aaa"), ("1235", "ccc"), ("1237", "bbb"))
            .map(s => async.unsafeRunAsync(reqq.enqueue1(s))(_ => IO.unit))
          v = resq.dequeue.take(3).compile.toList.unsafeRunSync()
        } yield v
        r.compile.toList.unsafeRunSync().flatten should be(List(Right("OK"), Right("OK"), Right("OK")))
      }
      "be possible to delete records using fs2 streaming" taggedAs WhenDynamodDBAvailable in {
        val piinguinClient = new PiinguinClient(SERVER_HOST, SERVER_PORT)
        val requestQueue   = Stream.eval(Queue.unbounded[IO, String])
        val resultQueue    = Stream.eval(Queue.unbounded[IO, Either[String, String]])
        val r: Stream[IO, List[Either[String, String]]] = for {
          reqq <- requestQueue
          resq <- resultQueue
          _ = async.unsafeRunAsync(piinguinClient.deleteRecordsStreamingIO(maxQueued = 2, reqq, resq))(_ => IO.unit)
          _ = List("1236", "1235", "1237")
            .map(s => async.unsafeRunAsync(reqq.enqueue1(s))(_ => IO.unit))
          v = resq.dequeue.take(3).compile.toList.unsafeRunSync()
        } yield v
        r.compile.toList.unsafeRunSync().flatten should be(List(Right("OK"), Right("OK"), Right("OK")))
      }
      "be possible to create a record using cats IO" taggedAs WhenDynamodDBAvailable in {
        val piinguinClient = new PiinguinClient(SERVER_HOST, SERVER_PORT)
        val result         = piinguinClient.createPiiRecordIO("1238", "567").unsafeRunSync()
        result should be(Right("OK"))
      }
    }
  }
  private var server: PiinguinServer = null

  /**
   * Start the server and create the table before all tests
   */
  override def beforeAll = {
    val client = DynamoDBTestClient.client
    try { client.deleteTable(TEST_TABLE_NAME) } catch { case r: ResourceNotFoundException => {} } // Make sure that the table does not exist
    DynamoDBTestUtils.createTable(client)(TEST_TABLE_NAME)('modifiedValue -> S)
    server = new PiinguinServer(SERVER_HOST, SERVER_PORT, TEST_TABLE_NAME, client)
    val _ = Future { server.start() }
  }

  /**
   * Delete the table
   */
  override def afterAll = {
    server.stop()
    val client = DynamoDBTestClient.client
    client.deleteTable(TEST_TABLE_NAME)
  }
}
