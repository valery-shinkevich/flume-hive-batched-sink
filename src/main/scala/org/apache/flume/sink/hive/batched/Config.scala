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

object Config {

  val HIVE_DATABASE = "hive.database"
  val HIVE_TABLE = "hive.table"
  val HIVE_PATH = "hive.path"
  val HIVE_PARTITION = "hive.partition"
  val HIVE_FILE_PREFIX = "hive.filePrefix"
  val HIVE_FILE_SUFFIX = "hive.fileSuffix"
  val HIVE_TIME_ZONE = "hive.timeZone"
  val HIVE_MAX_OPEN_FILES = "hive.maxOpenFiles"
  val HIVE_BATCH_SIZE = "hive.batchSize"
  val HIVE_IDLE_TIMEOUT = "hive.idleTimeout"
  val HIVE_IDLE_QUEUE_SIZE = "hive.idleQueueSize"
  val HIVE_IDLE_CLOSE_THREAD_POOL_SIZE = "hive.idleCloseThreadPoolSize"
  val HIVE_SERDE = "hive.serde"
  val HIVE_SERDE_PROPERTIES = "hive.serdeProperties"
  val HIVE_ROUND = "hive.round"
  val HIVE_ROUND_UNIT = "hive.roundUnit"
  val HIVE_ROUND_VALUE = "hive.roundValue"
  val HIVE_USE_LOCAL_TIMESTAMP = "hive.useLocalTimeStamp"
  val HIVE_SINK_COUNTER_TYPE = "hive.sinkCounterType"
  val Hive_ZOOKEEPER_CONNECT = "hive.zookeeperConnect"
  val HIVE_ZOOKEEPER_SESSION_TIMEOUT = "hive.zookeeperSessionTimeout"
  val HIVE_ZOOKEEPER_SERVICE_NAME = "hive.zookeeperServiceName"
  val HIVE_HOST_NAME = "hive.hostName"
  val HIVE_DB_CONNECT_URL = "hive.dbConnectURL"
  val HIVE_DTE_UPDATE_LOGDETAIL_URL = "hive.dte.updateLogDetailURL"
  val HIVE_DTE_LOGID:String = "hive.dte.logid"
  val HIVE_DTE_LOGDATE_FORMAT = "hive.dte.logdateFormat"

  object Default {
    val DEFAULT_DATABASE = "default"
    val DEFAULT_PARTITION = ""
    val DEFAULT_FILE_PREFIX = "FlumeData"
    val DEFAULT_FILE_SUFFIX = "orc"
    val DEFAULT_MAX_OPEN_FILES = 5000
    val DEFAULT_BATCH_SIZE = 1000
    val DEFAULT_IDLE_TIMEOUT: Long = 5000
    val DEFAULT_IDLE_QUEUE_SZIE = 100
    val DEFAULT_IDLE_CLOSE_THREAD_POOL_SIZE = 10
    val DEFAULT_ROUND = false
    val DEFAULT_ROUND_UNIT = "second"
    val DEFAULT_ROUND_VALUE = 1
    val DEFAULT_USE_LOCAL_TIMESTAMP = false
    val DEFAULT_SINK_COUNTER_TYPE = "SinkCounter"
    val DEFAULT_ZOOKEEPER_CONNECT: String = null
    val DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 5000
    val DEFAULT_DB_CONNECT_URL: String = null
    val DEFAULT_DTE_LOGDATE_FORMAT = "yyyyMMddHHmm"
  }

}
