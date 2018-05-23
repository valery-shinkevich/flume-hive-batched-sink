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

package org.apache.flume.sink.hive.batched.counter

import org.apache.flume.Event
import org.apache.flume.instrumentation.SinkCounter
import org.apache.flume.sink.hive.batched.util.TimedUtils
import org.apache.flume.sink.hive.batched.util.TimedUtils.{FiveMinLinkedHashMap, TimestampCount}

import scala.collection.mutable

object TimedSinkCounter {
  private val COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN = "sink.event.drain.sucess.5min"
  private val COUNTER_CATEGORY_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN = "sink.category.event.drain.sucess.5min"
  private val ATTRIBUTES = Array(COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN, COUNTER_CATEGORY_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN)
}

class TimedSinkCounter(val name: String) extends SinkCounter(name, TimedSinkCounter.ATTRIBUTES) with TimedSinkCounterMBean {

  private val eventDrainSuccessCountInFiveMinMap = FiveMinLinkedHashMap()
  private val categoryEventDrainSuccessCountInFiveMinMap: mutable.Map[String, mutable.Map[String, TimestampCount]] = {
    new mutable.HashMap[String, mutable.Map[String, TimedUtils.TimestampCount]]
  }

  def addToEventDrainSuccessCountInFiveMinMap(delta: Long): Unit = {
    TimedUtils.updateFiveMinMap(delta, eventDrainSuccessCountInFiveMinMap)
  }

  def getEventDrainSuccessCountInFiveMinJson: String = TimedUtils.convertFiveMinMapToJson(eventDrainSuccessCountInFiveMinMap)

  def getEventDrainSuccessCountInFiveMinMap: mutable.Map[String, TimestampCount] = eventDrainSuccessCountInFiveMinMap

  def addToCategoryEventDrainSuccessCountInFiveMinMap(events: List[Event], categoryKey: String): Unit = {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventDrainSuccessCountInFiveMinMap, categoryKey)
  }

  def addToCategoryEventDrainSuccessCountInFiveMinMap(events: List[Event]): Unit = {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventDrainSuccessCountInFiveMinMap)
  }

  def getCategoryEventDrainSuccessCountInFiveMinJson: String =
    TimedUtils.convertCategoryFiveMinMapToJson(categoryEventDrainSuccessCountInFiveMinMap)
}