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

package com.snowplowanalytics.piinguin.server
package implementations

// Scala
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

// GRPC
import io.grpc.stub.StreamObserver

// Specs2
import org.specs2._
import org.specs2.matcher.{AlwaysMatcher, FutureMatchers, MatchResult, Matcher}
import org.specs2.concurrent.ExecutionEnv

// THis project
import com.snowplowanalytics.piinguin.server.clients.{DynamoDBClient, DynamoDBContext}
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin._

class PiinguinImplSpec(implicit ee: ExecutionEnv) extends Specification with FutureMatchers with DynamoDBContext {
  def is = skipAllUnless(TestUtils.dynamoDbAvailable) ^ s2"""
  This is a specification for the Piinguin server implementation:

  createPiiRecord return a with no error for a request that successfully created a new record       $e1
  createPiiRecord return a with no error for a request that tries to create an existing record      $e2
  createPiiRecord return a with an error for a request that failed to create a record               $e3
  createPiiRecords return a stream of responses with the same results as createPiiRecord            $e4
  deletePiiRecord return with no error for a request that successfully deleted a record             $e5
  deletePiiRecord return a with an error for a request that fails to delete a record                $e6
  deletePiiRecords return a stream of responses with the same results as deletePiiRecord            $e7
  readPiiRecord return a record with no error for a request that successfully read a record         $e8
  readPiiRecord return no record and an error for a request that failed to read a record            $e9
  readPiiRecords return a stream of responses with the same results as readPiiRecord                $e10
  """

  def e1 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
  }

  def e2 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "9012")))) must be_==(
      new ChangeRecordResponse(Some(Status("Replaces PiiRecord(1234,5678)", true))))
      .await(retries = 0, timeout = 5 seconds)
  }
  def e3 = { dbc: DynamoDBClient =>
    dbc.client.deleteTable(dbc.tableName)
    val p      = new PiinguinImpl(dbc)
    val result = Await.result(p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))), 5 seconds)
    (result.status must beSome) and
      (result.status.get.isSuccess must beFalse) and
      (result.status.get.message must contain("Cannot do operations on a non-existent table"))
  }
  def e4 = { dbc: DynamoDBClient =>
    val p         = new PiinguinImpl(dbc)
    val responses = ListBuffer[ChangeRecordResponse]()
    var ready     = false
    val responseObserver = new StreamObserver[ChangeRecordResponse] {
      override def onError(t: Throwable): Unit = { /* fail test */ }

      override def onCompleted(): Unit = {}

      override def onNext(value: ChangeRecordResponse): Unit = responses += {
        value match {
          case crr: ChangeRecordResponse => { ready = true; crr }
        }
      }
    }
    val requests = p.createPiiRecords(responseObserver)
    requests.onNext(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678"))))
    requests.onCompleted
    while (!ready) { Thread.sleep(100L) }
    responses.head must be_==(new ChangeRecordResponse(Some(Status("OK", true))))
  }
  def e5 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
    p.deletePiiRecord(new DeletePiiRecordRequest(modifiedValue = "1234")) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
  }
  def e6 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    dbc.client.deleteTable(dbc.tableName)
    val result = Await.result(p.deletePiiRecord(new DeletePiiRecordRequest(modifiedValue = "1234")), 5 seconds)
    (result.status must beSome) and
      (result.status.get.isSuccess must beFalse) and
      (result.status.get.message must contain("Cannot do operations on a non-existent table"))
  }
  def e7 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
    val responses = ListBuffer[ChangeRecordResponse]()
    var ready     = false
    val responseObserver = new StreamObserver[ChangeRecordResponse] {
      override def onError(t: Throwable): Unit = { /* fail test */ }

      override def onCompleted(): Unit = {}

      override def onNext(value: ChangeRecordResponse): Unit = responses += {
        value match {
          case crr: ChangeRecordResponse => { ready = true; crr }
        }
      }
    }
    val requests = p.deletePiiRecords(responseObserver)
    requests.onNext(new DeletePiiRecordRequest(modifiedValue = "1234"))
    requests.onCompleted
    while (!ready) { Thread.sleep(100L) }
    responses.head must be_==(new ChangeRecordResponse(Some(Status("OK", true))))
  }
  def e8 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
    p.readPiiRecord(
      new ReadPiiRecordRequest(modifiedValue   = "1234",
                               processingBasis = ReadPiiRecordRequest.LawfulBasisForProcessing.CONSENT)) must be_==(
      new ReadPiiRecordResponse(Some(PiiRecord("1234", "5678")), Some(Status("OK", true))))
      .await(retries = 0, timeout = 5 seconds)
  }
  def e9 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    dbc.client.deleteTable(dbc.tableName)
    val result = Await.result(
      p.readPiiRecord(
        new ReadPiiRecordRequest(modifiedValue   = "1234",
                                 processingBasis = ReadPiiRecordRequest.LawfulBasisForProcessing.CONSENT)),
      5 seconds)
    (result.piiRecord must beNone) and
      (result.status must beSome) and
      (result.status.get.isSuccess must beFalse) and
      (result.status.get.message must contain("Cannot do operations on a non-existent table"))
  }
  def e10 = { dbc: DynamoDBClient =>
    val p = new PiinguinImpl(dbc)
    p.createPiiRecord(new CreatePiiRecordRequest(Some(PiiRecord("1234", "5678")))) must be_==(
      new ChangeRecordResponse(Some(Status("OK", true)))).await(retries = 0, timeout = 5 seconds)
    val responses = ListBuffer[ReadPiiRecordResponse]()
    var ready     = false
    val responseObserver = new StreamObserver[ReadPiiRecordResponse] {
      override def onError(t: Throwable): Unit = { /* fail test */ }

      override def onCompleted(): Unit = {}

      override def onNext(value: ReadPiiRecordResponse): Unit = responses += {
        value match {
          case rrr: ReadPiiRecordResponse => { ready = true; rrr }
        }
      }
    }
    val requests = p.readPiiRecords(responseObserver)
    requests.onNext(
      new ReadPiiRecordRequest(modifiedValue   = "1234",
                               processingBasis = ReadPiiRecordRequest.LawfulBasisForProcessing.CONSENT))
    requests.onCompleted
    while (!ready) { Thread.sleep(100L) }
    responses.head must be_==(new ReadPiiRecordResponse(Some(PiiRecord("1234", "5678")), Some(Status("OK", true))))
  }

}
