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

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.thrift.TException

import scala.collection.JavaConversions._


object HiveUtils extends LazyLogging {

  private val hiveConf = new HiveConf

  @throws[MetaException]
  def createHiveMetaStoreClient(hiveConf: HiveConf): HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)

  def closeHiveMetaStoreClient(client: HiveMetaStoreClient): Unit = {
    if (client != null) client.close()
  }

  @throws[TException]
  def addPartition(client: HiveMetaStoreClient, dbName: String, tableName: String, values: List[String], location: String): Unit = {
    val createTime = (System.currentTimeMillis / 1000).toInt
    val lastAccessTime = 0

    val parameters = new java.util.HashMap[String, String]

    val cols = getFields(client, dbName, tableName)
    val inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    val outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
    val compressed = false
    val numBuckets = -1
    val serDeInfoParameters = new java.util.HashMap[String, String]
    serDeInfoParameters.put("serialization.format", "1")
    val serDeInfo = new SerDeInfo(null, "org.apache.hadoop.hive.ql.io.orc.OrcSerde", serDeInfoParameters)
    val bucketCols = new java.util.ArrayList[String]
    val sortCols = new java.util.ArrayList[Order]
    val sdParameters = new java.util.HashMap[String, String]
    val sd = new StorageDescriptor(cols, location, inputFormat, outputFormat, compressed, numBuckets, serDeInfo, bucketCols, sortCols, sdParameters)
    val partition = new Partition(values, dbName, tableName, createTime, lastAccessTime, sd, parameters)
    val partitions = client.listPartitions(partition.getDbName, partition.getTableName, partition.getValues, 1.toShort)

    if (partitions.size != 0) {
      logger.info(String.format("partition already exist: %s.%s, %s", partition.getDbName, partition.getTableName, partition.getValues))
    }
    else {
      client.add_partition(partition)
    }
  }

  @throws[TException]
  def getFields(client: HiveMetaStoreClient, dbName: String, tableName: String): scala.List[FieldSchema] =
    client.getFields(dbName, tableName).toList

  @throws[TException]
  def getTable(client: HiveMetaStoreClient, dbName: String, tableName: String): Table = client.getTable(dbName, tableName)

  @throws[TException]
  def getTableColunmnProperties(client: HiveMetaStoreClient, dbName: String, tableName: String): Properties = {

    val properties = new Properties
    val fields = HiveUtils.getFields(client, dbName, tableName)
    val columnNameProperty = fields.map(_.getName).mkString(",")
    val columnTypeProperty = fields.map(_.getType).mkString(",")
    properties.setProperty(serdeConstants.LIST_COLUMNS, columnNameProperty)
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypeProperty)
    properties
  }

  @throws[TException]
  def addPartition(dbName: String, tableName: String, values: List[String], location: String): Unit = {
    var client: HiveMetaStoreClient = null
    try {
      client = createHiveMetaStoreClient(hiveConf)
      addPartition(client, dbName, tableName, values, location)
    } finally closeHiveMetaStoreClient(client)
  }

  @throws[TException]
  def getFields(dbName: String, tableName: String): scala.List[FieldSchema] = {
    var client: HiveMetaStoreClient = null
    try {
      client = createHiveMetaStoreClient(hiveConf)
      getFields(client, dbName, tableName)
    } finally closeHiveMetaStoreClient(client)
  }

  @throws[TException]
  def getTable(dbName: String, tableName: String): Table = {
    var client: HiveMetaStoreClient = null
    try {
      client = createHiveMetaStoreClient(hiveConf)
      getTable(client, dbName, tableName)
    } finally closeHiveMetaStoreClient(client)
  }

  @throws[TException]
  def getTableColunmnProperties(dbName: String, tableName: String): Properties = {
    var client: HiveMetaStoreClient = null
    try {
      client = createHiveMetaStoreClient(hiveConf)
      getTableColunmnProperties(client, dbName, tableName)
    } finally closeHiveMetaStoreClient(client)
  }

  def getPartitionValue(partition: String, key: String): String = {
    val LOGDATE_FLAG = key + "="
    var value: String = null
    if (partition.contains(LOGDATE_FLAG)) {
      value = partition.substring(partition.indexOf(LOGDATE_FLAG) + LOGDATE_FLAG.length)
      val i = value.indexOf("/")
      if (i > 0) value = value.substring(0, i)
    }
    value
  }

  def getPartitionValues(partition: String): scala.List[String] =
    partition.split("/")
      .map(part => part.split("=")(1)).toList
}
