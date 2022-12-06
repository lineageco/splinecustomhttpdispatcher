/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lineageco.splinecustomhttpdispatcher

object AzureSplineHeaders {
  private val Prefix = "ABSA-Spline"

  // Common
  val ApiVersion = s"$Prefix-API-Version"
  val ApiLTSVersion = s"$Prefix-API-LTS-Version"

  // Http specific
  val AcceptRequestEncoding = s"$Prefix-Accept-Request-Encoding"
  val Timeout = "X-SPLINE-TIMEOUT"
  val HttpSecurityKey = "X-FUNCTIONS-KEY"

  // Kafka specific
  val SplineEntityType = s"$Prefix-Entity-Type"
  val SpringClassId = "__TypeId__"
}
