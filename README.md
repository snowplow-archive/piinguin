# Piinguin

[![Build Status](https://travis-ci.org/snowplow-incubator/piinguin.svg?branch=master)](https://travis-ci.org/snowplow-incubator/piinguin)

The Piinguin project and the associate [snowplow-piinguin-relay][piinguin-relay] projects are meant to provide a way to handle PII in the Snowplow ecosystem. Recently in [snowplow/snowplow][snpl] we have added capabilities to pseudonymize PII fields. In addition there is a configurable way to emit those pseudonymous fields using the Snowplow EnrichEvent format to a specialized `pii` stream. These two projects are meant to act as a way to store and control access to the pii re-identification values. The relay project will run in AWS Lambda and forward all messages to the current project. The current project will store them and provide a controlled way to access them, which will require, in addition to the key sought, the justification with which reading PII is sought (e.g. LawfulBasisForProcessing.CONSENT) following the GDPR nomenclature.

This project is using [GRPC][grpc].

It consists of four submodules: protocols, client, server and e2e

Protocols (in `0-protocols`) contains the GRPC proto file and that is where the corresponding Scala files are generated.

Client (in `1-clients`) contains an example client in Scala. It contains Scala Future, FS2 IO and FS2 Streaming IO for the "verbs" supported by the protocol (i.e. create, read, delete). Create will update any existing fields that have the same key at the moment, although an alternative behavior is considered (adding a list of values corresponding to that key). The FS2 streaming IO is highly experimental and more than likely will change significantly in subsequent versions as we learn more about the right way to implement it. This client is currently used in the associated [snowplow-piinguin-relay][piinguin-relay] project.

Server (in `2-servers`) contains the server implementation, which uses the [scanamo][scanamo] library to read and write to AWS DynamoDB.

E2E (in `e2e`) contains a test that uses the client to talk to the server to read write and read to/from the test DynamoDB instance. That test also requires the 'dev/test environment' to be up.

## Using

### Server

The easiest way to use the server is to use the docker image from the [Snowplow docker repo][snowplow-docker].

Alternatively see below for an [example server run](#example-run-server).

### Client

You can either use the client library from this project if it suits your needs, or generate your own client code using GRPC and use it. For an example of using the code form this project, see [snowplow-piinguin-relay][piinguin-relay].

## Get dev/test environment up

```bash
vagrant up
...
vagrant ssh
```

## Run end-to-end test

```bash
sbt e2e/test
```

## Example run server

```bash
$ sbt "server/run -h 0.0.0.0 -p 8080 -t piinguin"
...
```

## Example client usage (the table must exist on dynamo to successfully write)

```scala
$ sbt "client/console"

scala> import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.{ExecutionContext, Await}

scala> import scala.concurrent.duration._
import scala.concurrent.duration._

scala> import com.snowplowanalytics.piinguin.client.PiinguinClient
import com.snowplowanalytics.piinguin.client.PiinguinClient

scala> implicit val ec = ExecutionContext.global
ec: scala.concurrent.ExecutionContextExecutor = scala.concurrent.impl.ExecutionContextImpl@7f4b9eca

scala> val c = new PiinguinClient("localhost", 8080)
c: com.snowplowanalytics.piinguin.client.PiinguinClient = com.snowplowanalytics.piinguin.client.PiinguinClient@6e5990ce

scala> val createResult = Await.result(c.createPiiRecord("123", "456"), 10 seconds)

createResult: Either[com.snowplowanalytics.piinguin.client.FailureMessage,com.snowplowanalytics.piinguin.client.SuccessMessage] = Left(FailureMessage(UNAVAILABLE: io exception))

or

createResult: Either[com.snowplowanalytics.piinguin.client.FailureMessage,com.snowplowanalytics.piinguin.client.SuccessMessage] = Left(FailureMessage(Cannot do operations on a non-existent table (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: ResourceNotFoundException; Request ID: 287894d3-ced6-4382-ab9b-bb4f51a0070c)))

or

createResult: Either[com.snowplowanalytics.piinguin.client.FailureMessage,com.snowplowanalytics.piinguin.client.SuccessMessage] = Right(SuccessMessage(OK))

scala> import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin.ReadPiiRecordRequest.LawfulBasisForProcessing
scala> val readResult = Await.result(c.readPiiRecord("123", LawfulBasisForProcessing.CONSENT), 10 seconds)

                                                                   ^
readResult: Either[com.snowplowanalytics.piinguin.client.FailureMessage,com.snowplowanalytics.piinguin.client.PiinguinClient.PiiRecord] = Right(PiiRecord(123,456))


```

## Copyright and license

Copyright 2012-2018 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0

[piinguin-relay]: https://github.com/snowplow-incubator/snowplow-piinguin-relay
[grpc]: https://grpc.io/
[scanamo]: https://github.com/scanamo/scanamo
[snowplow-docker]: https://bintray.com/snowplow/registry
[snpl]: https://github.com/snowplow/snowplow
[scanamo]: https://github.com/scanamo/scanamo
