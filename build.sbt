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

lazy val root = (project in file("."))
  .settings(BuildSettings.basicSettings)
  .aggregate(client, server)


lazy val client = (project in file("1-clients"))
  .settings(
    BuildSettings.clientSettings)
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.buildInfo)

lazy val server = (project in file("2-servers"))
  .settings(
    BuildSettings.serverSettings
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.buildInfo)

lazy val e2e = (project in file("e2e"))
  .settings(
    BuildSettings.e2eTestSettings
  )
  .dependsOn(server % "test->test", client)
