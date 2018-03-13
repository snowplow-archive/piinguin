package com.snowplowanalytics.piinguin.server

import scala.util.{Failure, Success, Try}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import clients.DynamoDBTestClient
import clients.DynamoDBTestUtils

object TestUtils {
  private val connectivityTestTable = "can-write-test"

  def dynamoDbAvailable: Boolean = {
    deleteTestTable()
    Try(DynamoDBTestUtils.createTable(client)(connectivityTestTable)('modifiedValue -> S)) match {
      case Success(_) => deleteTestTable(); true
      case Failure(t) => println(s"DynamoDB test client unable to connect and write: $t"); false
    }
  }
  private def deleteTestTable() = Try(DynamoDBTestUtils.deleteTable(client)(connectivityTestTable)); {}
  def client =
    AmazonDynamoDBClient
      .builder()
      .withCredentials(new AWSStaticCredentialsProvider(DynamoDBTestClient.credentials))
      .withEndpointConfiguration(DynamoDBTestClient.endpointConfiguration)
      .build()
}
