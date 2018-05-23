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

import com.typesafe.scalalogging.LazyLogging
import org.apache.flume.sink.hive.batched.Callback
import org.apache.flume.sink.hive.batched.util.HiveUtils
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.thrift.TException

class AddPartitionCallback(var dbName: String, var tableName: String, var values: List[String], var location: String) extends Callback with LazyLogging {
  def run(): Unit = {
    try
      HiveUtils.addPartition(dbName, tableName, values, location)
    catch {
      case e: AlreadyExistsException =>
        logger.warn("Partition already exists: " + dbName + "." + tableName + " " + values)
      case e: TException =>
        logger.error("Fail to add partition: " + dbName + "." + tableName + " " + values, e)
    }
  }
}

