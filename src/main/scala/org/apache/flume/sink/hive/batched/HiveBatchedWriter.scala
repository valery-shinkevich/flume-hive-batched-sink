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

package org.apache.flume.sink.hive.batched

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.{AbstractDeserializer, SerDeException}
import org.apache.hadoop.io.Text

class HiveBatchedWriter(val conf: Configuration, var deserializer: AbstractDeserializer, var file: String) {

  private val writer = {
    val writerOptions: OrcFile.WriterOptions = OrcFile.writerOptions(conf)
    writerOptions.inspector(deserializer.getObjectInspector)
    OrcFile.createWriter(new Path(file), writerOptions)
  }

  //if (initCallbacks != null) initCallbacks.foreach(_.run())

  private var lastWriteTime: Long = -1 // -1: no data write yet

  private var idleTimeout: Long = 5000
  private var initCallbacks: List[Callback] = Nil
  private var closeCallbacks: List[Callback] = Nil

  private var logdate: String = _ // optional
  private var logdateFormat: String = _

  private var minFinishedTimestamp: Long = 0

  @throws[IOException]
  @throws[SerDeException]
  def append(bytes: Array[Byte]): Unit = {
    writer.addRow(deserializer.deserialize(new Text(bytes)))
    lastWriteTime = System.currentTimeMillis
  }

  @throws[IOException]
  def close(): Unit = {
    writer.close()
    if (closeCallbacks != null) closeCallbacks.foreach(_.run)
  }

  def isIdle: Boolean = {
    val currentTimestamp = System.currentTimeMillis
    lastWriteTime > 0 && currentTimestamp > minFinishedTimestamp && currentTimestamp - lastWriteTime >= idleTimeout
  }

  def setLogdate(logdate: String): Unit = {
    this.logdate = logdate
  }

  def setLogdateFormat(logdateFormat: String): Unit = {
    this.logdateFormat = logdateFormat
  }

  def setIdleTimeout(idleTimeout: Long): Unit = {
    this.idleTimeout = idleTimeout
  }

  def setInitCallbacks(initCallbacks: List[Callback]): Unit = {
    this.initCallbacks = initCallbacks
  }

  def setCloseCallbacks(closeCallbacks: List[Callback]): Unit = {
    this.closeCallbacks = closeCallbacks
  }

  def setMinFinishedTimestamp(minFinishedTimestamp: Long): Unit = {
    this.minFinishedTimestamp = minFinishedTimestamp
  }

  def getFile: String = file
}