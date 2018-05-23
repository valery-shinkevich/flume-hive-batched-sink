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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.text.{ParseException, SimpleDateFormat}
import java.util.Calendar

object CommonUtils {
  def getStackTraceStr(exception: Exception): String = {
    val stream = new ByteArrayOutputStream
    exception.printStackTrace(new PrintStream(stream))
    stream.toString
  }

  @throws[ParseException]
  def convertTimeStringToTimestamp(timeString: String, timeFormat: String): Long = {
    val sdf = new SimpleDateFormat(timeFormat)
    sdf.parse(timeString).getTime
  }

  def getMillisecond(num: Long, unit: Int): Long = {
    if (unit == Calendar.SECOND) return num * 1000
    else if (unit == Calendar.MINUTE) return num * 60 * 1000
    else if (unit == Calendar.HOUR_OF_DAY) return num * 3600 * 1000
    throw new IllegalArgumentException("unknown time unit: " + unit)
  }
}