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

import java.sql._

class DBManager(val connectURL: String) {
  private var conn :Connection = _
  private var stmt : Statement = _

  @throws[SQLException]
  def connect(): Unit = {
    conn  = DriverManager.getConnection(this.connectURL)
    stmt = conn.createStatement()
  }

  @throws[SQLException]
  def execute(sql: String): Unit = {
    stmt.execute(sql)
  }

  @throws[SQLException]
  def executeQuery(sql: String): ResultSet = stmt.executeQuery(sql)

  @throws[SQLException]
  def close(): Unit = {
    if (conn != null) conn.close
  }
}

