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

package com.snowplowanalytics.piinguin.client

// Scala
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// GRPC
import io.grpc.CallOptions
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.stub.StreamObserver

// FS2
import fs2.Sink
import fs2.async
import fs2.async.mutable.Queue

// Cats effect
import cats.effect.IO

// Slf4j
import org.slf4j.LoggerFactory

// Generated
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin._
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin.ReadPiiRecordRequest.LawfulBasisForProcessing
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin.PiinguinGrpc.PiinguinStub

/**
 * Piinguin client implementation following the piinguin GRPC protocol
 * @param host the server host
 * @param port the server port
 */
class PiinguinClient(host: String, port: Int)(implicit ec: ExecutionContext) {

  private val logger      = LoggerFactory.getLogger(classOf[PiinguinClient].getName)
  private val channel     = NettyChannelBuilder.forAddress(host, port).negotiationType(NegotiationType.PLAINTEXT).build()
  private val callOptions = CallOptions.DEFAULT.withWaitForReady()
  private val stub        = new PiinguinStub(channel, callOptions)

  /**
   * createPiiRecord creates a single piirecord using a single request asynchronously
   * @param modifiedValue the modified value of the record
   * @param originalValue the original value for the record
   * @return a future for the execution that returns either a failure message or a success message
   */
  def createPiiRecord(modifiedValue: String, originalValue: String): Future[Either[String, String]] = {
    logger.debug(s"Sending single create record request for modifiedValue: $modifiedValue originalValue: originalValue")
    stub.createPiiRecord(CreatePiiRecordRequest(Some(PiiRecord(modifiedValue, originalValue)))).transform {
      returnChangeRecordResponse(_)
    }
  }

  /**
   * deletePiiRecord deletes a single piirecord using a single request asynchronously
   * @param modifiedValue the modified value of the record
   * @return a future for the execution that returns either a failure message or a success message
   */
  def deletePiiRecord(modifiedValue: String): Future[Either[String, String]] = {
    logger.debug(s"Sending single delete record request for modifiedValue: $modifiedValue")
    stub.deletePiiRecord(DeletePiiRecordRequest(modifiedValue)).transform { returnChangeRecordResponse(_) }
  }

  private def returnChangeRecordResponse(response: Try[ChangeRecordResponse]) = response match {
    case Success(ChangeRecordResponse(Some(status))) =>
      if (status.isSuccess) okToSuccess(status.message) else errToSuccess(status.message)
    case Success(ChangeRecordResponse(None)) => errToSuccess("Failed to get a response")
    case Failure(t)                          => errToSuccess(t.getMessage)
  }

  /**
   * readPiiRecord reads a single piirecord using a single request asynchronously
   * @param modifiedValue the modified value of the record
   * @param basisForProcessing the legal basis for requesting to read this record
   * @return a future for the execution that returns either a failure message or a pii record
   */
  def readPiiRecord(modifiedValue: String,
                    basisForProcessing: LawfulBasisForProcessing): Future[Either[String, PiiRecord]] = {
    logger.debug(
      s"Sending single read record request for modifiedValue: $modifiedValue and basisForProcessing: $basisForProcessing")
    stub.readPiiRecord(ReadPiiRecordRequest(modifiedValue, basisForProcessing)).transform {
      case Success(ReadPiiRecordResponse(_, Some(status))) if (!status.isSuccess) => errToSuccess(status.message)
      case Success(ReadPiiRecordResponse(Some(piiRecord), _)) => Success(Right(piiRecord))
      case Failure(t)                                         => errToSuccess(t.getMessage)
    }
  }

  /**
   * createPiiRecordIO cats IO version of createPiiRecord call
   */
  def createPiiRecordIO(modifiedValue: String, originalValue: String): IO[Either[String, String]] = IO.fromFuture {
    IO { createPiiRecord(modifiedValue, originalValue) }
  }

  /**
   * deletePiiRecordIO cats IO version of deletePiiRecord
   */
  def deletePiiRecordIO(modifiedValue: String): IO[Either[String, String]] = IO.fromFuture {
    IO { deletePiiRecord(modifiedValue) }
  }

  /**
   * readPiiRecordIO cats IO version of readPiiRecord
   */
  def readPiiRecordIO(modifiedValue: String,
                      basisForProcessing: LawfulBasisForProcessing): IO[Either[String, PiiRecord]] = IO.fromFuture {
    IO { readPiiRecord(modifiedValue, basisForProcessing) }
  }

  /**
   * EXPERIMENTAL
   * Given two fs2 queues, it consumes requests from one and enqueues responses in the other.
   *
   * @param maxQueued the maximum requests to be queued before the request queue blocks
   * @param kvPairs request queue
   * @param responseQueue response queue
   */
  def createRecordsStreamingIO(maxQueued: Int,
                               kvPairs: Queue[IO, (String, String)],
                               responseQueue: Queue[IO, Either[String, String]]): IO[Unit] = {
    val responseObserver = getResponseObserverChangeRecord(responseQueue)
    val requestObserver  = stub.createPiiRecords(responseObserver)
    kvPairs.dequeue
      .observeAsync(maxQueued)(Sink(requestSinkCreatePiiRecord(requestObserver)))
      .compile
      .drain
  }

  /**
   * EXPERIMENTAL
   * Given two fs2 queues, it consumes requests from one and enqueues responses in the other.
   *
   * @param maxQueued the maximum requests to be queued before the request queue blocks
   * @param keys request queue
   * @param responseQueue response queue
   */
  def deleteRecordsStreamingIO(maxQueued: Int,
                               keys: Queue[IO, String],
                               responseQueue: Queue[IO, Either[String, String]]): IO[Unit] = {
    val responseObserver = getResponseObserverChangeRecord(responseQueue)
    val requestObserver  = stub.deletePiiRecords(responseObserver)
    keys.dequeue
      .observeAsync(maxQueued)(Sink(requestSinkDeletePiiRecord(requestObserver)))
      .compile
      .drain
  }

  /**
   * EXPERIMENTAL
   * Given two fs2 queues, it consumes requests from one and enqueues responses in the other.
   *
   * @param maxQueued the maximum requests to be queued before the request queue blocks
   * @param keysAndJustification request queue
   * @param responseQueue response queue
   */
  def readRecordsStreamingIO(maxQueued: Int,
                             keysAndJustification: Queue[IO, (String, LawfulBasisForProcessing)],
                             responseQueue: Queue[IO, Either[String, PiiRecord]]): IO[Unit] = {
    val responseObserver = getResponseObserverReadRecord(responseQueue)
    val requestObserver  = stub.readPiiRecords(responseObserver)
    keysAndJustification.dequeue
      .observeAsync(maxQueued)(Sink(requestSinkReadPiiRecord(requestObserver)))
      .compile
      .drain
  }

  private def getResponseObserverChangeRecord(responseQueueIO: Queue[IO, Either[String, String]]) =
    new StreamObserver[ChangeRecordResponse] {

      def enqueue(either: Either[String, String]): Unit =
        async.unsafeRunAsync(responseQueueIO.enqueue1(either))(_ => IO.unit)

      override def onError(t: Throwable): Unit = enqueue(err(s"${t.getMessage}"))

      override def onCompleted(): Unit = {}

      override def onNext(value: ChangeRecordResponse): Unit =
        enqueue(
          value match {
            case ChangeRecordResponse(None)         => err("Failed to get a response")
            case ChangeRecordResponse(Some(status)) => if (status.isSuccess) ok(status.message) else err(status.message)
          }
        )
    }
  private def getResponseObserverReadRecord(responseQueueIO: Queue[IO, Either[String, PiiRecord]]) =
    new StreamObserver[ReadPiiRecordResponse] {

      def enqueue(either: Either[String, PiiRecord]): Unit =
        async.unsafeRunAsync(responseQueueIO.enqueue1(either))(_ => IO.unit)

      override def onError(t: Throwable): Unit = enqueue(err(s"${t.getMessage}"))

      override def onCompleted(): Unit = {}

      override def onNext(value: ReadPiiRecordResponse): Unit =
        enqueue(
          value match {
            case ReadPiiRecordResponse(_, Some(status)) if (!status.isSuccess) => err(status.message)
            case ReadPiiRecordResponse(Some(piiRecord), _) => ok(piiRecord)
          }
        )
    }

  private def requestSinkCreatePiiRecord(requestObserver: StreamObserver[CreatePiiRecordRequest]) =
    (kv: (String, String)) =>
      IO {
        val (modifiedValue: String, originalValue: String) = kv
        requestObserver.onNext(CreatePiiRecordRequest(Some(PiiRecord(modifiedValue, originalValue))))
    }

  private def requestSinkDeletePiiRecord(requestObserver: StreamObserver[DeletePiiRecordRequest]) =
    (key: String) =>
      IO {
        requestObserver.onNext(DeletePiiRecordRequest(key))
    }

  private def requestSinkReadPiiRecord(requestObserver: StreamObserver[ReadPiiRecordRequest]) =
    (keysAndJustification: (String, LawfulBasisForProcessing)) =>
      IO {
        val (key: String, basisForProcessing: LawfulBasisForProcessing) = keysAndJustification
        requestObserver.onNext(ReadPiiRecordRequest(key, basisForProcessing))
    }

  /**
   * Private methods returning the result of the execution
   */
  private def okToSuccess(msg: String)  = Success(ok(msg))
  private def errToSuccess(msg: String) = Success(err(msg))

  private def ok(msg: String)    = Right(msg)
  private def ok(pii: PiiRecord) = Right(pii)
  private def err(msg: String)   = Left(msg)
}
