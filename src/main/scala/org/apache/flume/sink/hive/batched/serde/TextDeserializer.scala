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

package org.apache.flume.sink.hive.batched.serde

import java.util
import java.util.Properties

import org.apache.flume.sink.hive.batched.util.HiveUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.{AbstractDeserializer, SerDeException, SerDeStats}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoFactory, TypeInfoUtils}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.thrift.TException

import scala.collection.JavaConversions._

abstract class TextDeserializer extends AbstractDeserializer {
  private var rowOI: ObjectInspector = _
  private val row = new util.ArrayList[AnyRef]

  @throws[TException]
  @throws[SerDeException]
  def initializeByTableName(configuration: Configuration, dbName: String, tableName: String): Unit = {
    val tbl = HiveUtils.getTableColunmnProperties(dbName, tableName)
    initialize(configuration, tbl)
  }

  @throws[SerDeException]
  override def initialize(configuration: Configuration, tbl: Properties): Unit = {
    val columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS)
    val columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES)
    initialize(columnNameProperty, columnTypeProperty)
  }

  def initialize(columnNameProperty: String, columnTypeProperty: String): Unit = {
    val columnNames = columnNameProperty.split(",").toList
    val columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty)
    val rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes).asInstanceOf[StructTypeInfo]
    rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo)
  }

  @throws[SerDeException]
  override def deserialize(writable: Writable): AnyRef = {
    row.clear()
    this.deserialize(writable.asInstanceOf[Text].getBytes, row)
  }

  @throws[SerDeException]
  override def getObjectInspector: ObjectInspector = rowOI

  override def getSerDeStats: SerDeStats = null

  def deserialize(bytes: Array[Byte], reuse: util.List[AnyRef]): AnyRef
}