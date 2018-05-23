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

package org.apache.flume.sink.hive.batched.callback

import java.sql.SQLException

import com.typesafe.scalalogging.LazyLogging
import org.apache.flume.instrumentation.SinkCounter
import org.apache.flume.sink.hive.batched.Callback
import org.apache.flume.sink.hive.batched.counter.TimedSinkCounter
import org.apache.flume.sink.hive.batched.dao.HiveSinkDetailDao
import org.apache.flume.sink.hive.batched.util.CommonUtils

class UpdateSinkDetailCallback(val connectURL: String, val name: String, val logdate: String, val hostName: String, val sinkCounter: SinkCounter) extends Callback with LazyLogging {

  def run(): Unit = {

    val dao = new HiveSinkDetailDao(connectURL, name)
    try {
      dao.connect()
      val receiveCount = 0
      val sinkCount = {
        if (sinkCounter != null && sinkCounter.isInstanceOf[TimedSinkCounter]) {
          val cntMap = sinkCounter.asInstanceOf[TimedSinkCounter].getEventDrainSuccessCountInFiveMinMap

          if (cntMap.contains(logdate)) cntMap(logdate).count else 0
        } else {
          0
        }
      }

      val updateTimestamp = System.currentTimeMillis

      if (dao.exists(logdate, hostName)) {
        dao.update(logdate, hostName, receiveCount, sinkCount, updateTimestamp)
      }
      else {
        dao.create(logdate, hostName, receiveCount, sinkCount, updateTimestamp)
      }
    } catch {
      case e: SQLException =>
        logger.error(CommonUtils.getStackTraceStr(e))
    } finally if (dao != null) dao.close()

  }
}