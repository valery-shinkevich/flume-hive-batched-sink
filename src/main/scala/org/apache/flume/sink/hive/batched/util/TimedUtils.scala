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

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flume.Event

import scala.collection.mutable

object TimedUtils {

  private val FIVE_MIN_TIME_FORMAT = "yyyyMMddHHmm"

  private val fiveMinSDF = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat(FIVE_MIN_TIME_FORMAT)
  }

  private val EVENT_CATEGORY_KEY = "category"
  private val EVENT_TIMESTAMP_KEY = "timestamp"
  private val NO_CATEGORY = "no_category"
  private val NO_TIMESTAMP = "no_timestamp"
  private val INVALID_TIMESTAMP = "invalid_timestamp"
  private val FIVE_MIN_MAP_TYPE = new TypeToken[Nothing]() {}.getType
  private val CATEGORY_FIVE_MIN_MAP_TYPE = new TypeToken[Nothing]() {}.getType
  private val gson = new Gson

  def convertTimestampToFiveMinStr(timestamp: Long): String = {
    val fiveMinTimestamp = Math.floor(timestamp / 300000).toLong * 300000
    fiveMinSDF.get.format(new Date(fiveMinTimestamp))
  }

  def convertTimestampStrToFiveMinStr(timestampStr: String): String = convertTimestampToFiveMinStr(timestampStr.toLong)

  def convertFiveMinMapToJson(fiveMinMap:  mutable.Map[String, TimestampCount]): String = gson.toJson(fiveMinMap, FIVE_MIN_MAP_TYPE)

  def convertCategoryFiveMinMapToJson(fiveMinMap: mutable.Map[String, mutable.Map[String, TimestampCount]]): String = gson.toJson(fiveMinMap, CATEGORY_FIVE_MIN_MAP_TYPE)

  def updateFiveMinMap(delta: Long, fiveMinMap: mutable.Map[String, TimestampCount]): Unit = {

    val timestamp = System.currentTimeMillis
    val fiveMin = convertTimestampToFiveMinStr(timestamp)

    synchronized(fiveMinMap) {

      if (!fiveMinMap.contains(fiveMin))
        fiveMinMap.put(fiveMin, TimestampCount())

      fiveMinMap(fiveMin).addToCountAndTimestamp(delta, timestamp)
      ""
    }
  }

  def updateCategoryFiveMinMap(events: List[Event], fiveMinMap: mutable.Map[String, mutable.Map[String, TimestampCount]]): Unit = {
    updateCategoryFiveMinMap(events, fiveMinMap, EVENT_CATEGORY_KEY)
  }

  def updateCategoryFiveMinMap(events: List[Event], fiveMinMap: mutable.Map[String, mutable.Map[String, TimestampCount]], categoryKey: String): Unit = {

    if (events == null || events.isEmpty) return

    val counters = new mutable.HashMap[String, Long]()

    events.toStream.foreach(event => {
      val headers = event.getHeaders
      val category = if (headers.containsKey(categoryKey)) headers.get(categoryKey)
      else NO_CATEGORY
      var fiveMin = NO_TIMESTAMP
      if (headers.containsKey(EVENT_TIMESTAMP_KEY)) {
        val timestampStr = headers.get(EVENT_TIMESTAMP_KEY)
        try
          fiveMin = convertTimestampStrToFiveMinStr(timestampStr)
        catch {
          case e: Exception =>
            fiveMin = INVALID_TIMESTAMP
        }
      }
      val key = category + "\t" + fiveMin
      if (!counters.contains(key)) counters.put(key, 0L)
      counters.put(key, counters(key) + 1)
    })

    val updateTimestamp = System.currentTimeMillis
    synchronized(fiveMinMap) {
      counters.foreach { case (key, num) => {
        val arr = key.split("\t")
        val category = arr(0)
        val fiveMin = arr(1)
        if (!fiveMinMap.contains(category)) fiveMinMap.put(category, FiveMinLinkedHashMap())
        if (!fiveMinMap.get(category).contains(fiveMin)) fiveMinMap.get(category).get.put(fiveMin, TimestampCount())
        fiveMinMap.get(category).get(fiveMin).addToCountAndTimestamp(num, updateTimestamp)
      }
      }
      ""
    }
  }

  object FiveMinLinkedHashMap {

    private val DEFAULT_MAX_SIZE = 500

    def apply(): FiveMinLinkedHashMap = FiveMinLinkedHashMap(DEFAULT_MAX_SIZE)

  }

  case class FiveMinLinkedHashMap(maxSize: Int) extends mutable.LinkedHashMap[String, TimestampCount] {
    protected def removeEldestEntry(eldest: Int): Boolean = size > maxSize
  }

  case class TimestampCount(var count: Long, var timestamp: Long) {
    def addToCountAndTimestamp(delta: Long, timestamp: Long): Long = {
      this.count += delta
      this.timestamp = timestamp
      this.count
    }

  }

  object TimestampCount {
    def apply(): TimestampCount = TimestampCount(0, System.currentTimeMillis)
  }

}
