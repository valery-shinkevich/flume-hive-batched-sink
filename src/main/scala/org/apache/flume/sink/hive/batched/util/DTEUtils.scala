/**
Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.
 You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
  **/

package org.apache.flume.sink.hive.batched.util

import com.sun.jersey.api.client.Client
import com.typesafe.scalalogging.LazyLogging

object DTEUtils extends LazyLogging{

  def updateLogDetail(serviceURL: String, logid: Int, logdata: String): Unit = {
    try {
      val url = s"$serviceURL/$logid/$logdata"
      val client = Client.create
      val resource = client.resource(url)
      resource.post()
    } catch {
      case e: Throwable =>
        logger.error(s"Fail to update DTE LogDetail ($logid, $logdata)", e)
    }
  }
}
