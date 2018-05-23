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

package org.apache.flume.sink.hive.batched.dao

import java.sql.SQLException
import java.util

import org.apache.flume.sink.hive.batched.util.DBManager

import scala.collection.JavaConversions._


class HiveSinkDetailDao(val connectURL: String, var name: String) extends AutoCloseable {

  private val dbManager = new DBManager(connectURL)
  final private val TABLE_NAME = "hive_sink_detail"

  @throws[SQLException]
  def connect(): Unit = {
    dbManager.connect()
  }

  @throws[SQLException]
  override def close(): Unit = {
    dbManager.close()
  }

  @throws[SQLException]
  def getFinishedLogdateList(onlineServerNum: Int): List[String] = {
    val sql =
      s"""SELECT t.logdate AS logdate FROM (
         |  SELECT logdate, COUNT(*) AS n FROM $TABLE_NAME
         |  WHERE state='NEW' AND name='$name'
         |  GROUP BY logdate
         |) t
         |WHERE t.n >= $onlineServerNum""".stripMargin

    val logdateList = new util.ArrayList[String]

    val rs = dbManager.executeQuery(sql)
    try
        while ( {
          rs.next
        }) logdateList.add(rs.getString("logdate"))
    finally if (rs != null) rs.close()

    logdateList.toList
  }

  @throws[SQLException]
  def updateCheckedState(logdateList: List[String]): Unit = {
    if (logdateList.size == 0) return
    val logdateSQL = logdateList.mkString("'", "', '", "'")
    //  "'" + Joiner.on("', '").join(logdateList) + "'"
    val sql = s"UPDATE $TABLE_NAME SET state='CHECKED' WHERE state='NEW' AND name='$name' AND logdate in ($logdateSQL)"
    dbManager.execute(sql)
  }

  @throws[SQLException]
  def exists(logdate: String, hostName: String): Boolean = {
    val sql = s"SELECT * FROM $TABLE_NAME WHERE name='$name' AND logdate='$logdate' AND hostname='$hostName'"
    val rs = dbManager.executeQuery(sql)
    try
      rs.next
    catch {
      case _: Throwable => false
    }
    finally if (rs != null) rs.close()
  }

  @throws[SQLException]
  def create(logdate: String, hostName: String, receiveCount: Long, sinkCount: Long, updateTimestamp: Long): Unit = {
    val sql =
      s"""INSERT INTO $TABLE_NAME(name, logdate, hostname, receivecount, sinkcount, updatetime)
         | VALUES('$name', '$logdate', '$hostName', '$receiveCount', '$sinkCount', '$updateTimestamp')""".stripMargin
    dbManager.execute(sql)
  }

  @throws[SQLException]
  def update(logdate: String, hostName: String, receiveCount: Long, sinkCount: Long, updateTimestamp: Long): Unit = {
    val sql =
      s"""UPDATE $TABLE_NAME SET receivecount='$receiveCount', sinkcount='$sinkCount', updatetime='$updateTimestamp'
         | WHERE name='$name' AND logdate='$logdate' AND hostname='$hostName'""".stripMargin
    dbManager.execute(sql)
  }
}