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
import java.sql.SQLException
import java.text.ParseException
import java.util.Map.Entry
import java.util._
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import com.google.common.base.Preconditions
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.scalalogging.LazyLogging
import org.apache.flume.Sink.Status
import org.apache.flume.conf.Configurable
import org.apache.flume.formatter.output.BucketPath
import org.apache.flume.instrumentation.SinkCounter
import org.apache.flume.sink.AbstractSink
import org.apache.flume.sink.hive.batched.callback.{AddPartitionCallback, UpdateSinkDetailCallback}
import org.apache.flume.sink.hive.batched.counter.TimedSinkCounter
import org.apache.flume.sink.hive.batched.dao.HiveSinkDetailDao
import org.apache.flume.sink.hive.batched.util.{CommonUtils, DTEUtils, HiveUtils}
import org.apache.flume.sink.hive.batched.zk.{ZKService, ZKServiceException}
import org.apache.flume.{Context, Event, EventDeliveryException, Sink}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.{AbstractDeserializer, Deserializer, SerDeException}

import scala.collection.mutable

object HiveBatchedSink {
  private val DIRECTORY_DELIMITER = System.getProperty("file.separator")
}

class HiveBatchedSink extends AbstractSink with Configurable with LazyLogging {

  private var conf: Configuration = null
  private var isRunning = false
  // Hive
  private var dbName: String = null
  private var tableName: String = null
  private var partition: String = null
  private var path: String = null
  private var fileName: String = null
  private var suffix: String = null
  private var timeZone: TimeZone = null
  private var deserializer: AbstractDeserializer = null
  // Transaction
  private var batchSize = 0
  // Rounding
  private var needRounding = false
  private var roundUnit = Calendar.SECOND
  private var roundValue = 1
  private var useLocalTime = false
  // Active writers
  private var maxOpenFiles = 0
  private var writerCounter: AtomicLong = null
  private var activeWriters: WriterLinkedHashMap = null

  // Idle writers
  private var idleTimeout = 0L
  private var idleQueueSize = 0
  private var idleWriterCloseThreadPoolSize = 0
  private var idleWriterRemoveThread: IdleWriterRemoveThread = null
  private var idleWriters: BlockingQueue[HiveBatchedWriter] = null
  private var idleWriterCloseThreadPool: ExecutorService = null
  // Counter
  private var timedSinkCounterCategoryKey = "category"
  private var sinkCounterType = "SinkCounter"
  private var sinkCounter: SinkCounter = null
  // Zookeeper
  private var zookeeperConnect: String = null
  private var zookeeperSessionTimeout = 0
  private var zookeeperServiceName: String = null
  private var hostName: String = null
  private var zkService: ZKService = null
  // DTE
  private var dbConnectURL: String = null
  private var updateLogDetailURL: String = null
  private var logId = 0
  private var logdateFormat: String = null
  private var leaderThread: LeaderThread = null

  private class WriterLinkedHashMap(val maxOpenFiles: Int) extends LinkedHashMap[String, HiveBatchedWriter] {
    override protected def removeEldestEntry(eldest: Entry[String, HiveBatchedWriter]): Boolean = {
      synchronized {
        if (this.size > maxOpenFiles) {
          try
            idleWriters.put(eldest.getValue)
          catch {
            case e: InterruptedException =>
              logger.warn("interrupted", e)
          }
          true
        }
        else false
      }
    }
  }

  private class IdleWriterRemoveThread extends Runnable {

    final private val CHECK_INTERVAL: Long = 5

    override def run(): Unit = {
      while ( {
        isRunning && !Thread.currentThread.isInterrupted
      }) {
        synchronized {


          val it = activeWriters.entrySet.iterator
          while ( {
            it.hasNext
          }) {
            val entry = it.next
            if (entry.getValue.isIdle) {
              try // put writer to idleWriters
              idleWriters.put(entry.getValue)
              catch {
                case e: InterruptedException =>
                  logger.warn("interrupted", e)
                  Thread.currentThread.interrupt()
              }
              // remove writer from activeWriters
              it.remove()
            }
          }
        }

        try
          TimeUnit.SECONDS.sleep(CHECK_INTERVAL)
        catch {
          case e: InterruptedException =>
            logger.warn("interrupted", e)
            Thread.currentThread.interrupt()
        }
      }
    }
  }

  private class IdleWriterCloseThread extends Runnable {
    override def run(): Unit = {
      while ( {
        isRunning && !Thread.currentThread.isInterrupted
      }) try {
        val writer = idleWriters.take
        logger.info("Closing " + writer.getFile)
        try
          writer.close()
        catch {
          case e: Exception =>
            logger.error("Fail to close " + writer.getFile, e)
        }
      } catch {
        case e: InterruptedException =>
          logger.warn("interrupted", e)
          Thread.currentThread.interrupt()
      }
    }
  }

  private class LeaderThread extends Runnable {
    final private val CHECK_INTERVAL = 5

    override def run(): Unit = {
      while ( {
        isRunning && !Thread.currentThread.isInterrupted
      }) {
        try {
          val leader = zkService.getLeader
          if (leader != null && leader.hostName.equals(hostName)) { // do some leader job
            val onlineServerNum = zkService.getAllServerInfos.size
            updateLogDetail(onlineServerNum)
          }
        } catch {
          case e: Exception =>
            logger.error(CommonUtils.getStackTraceStr(e))
        }
        try
          TimeUnit.SECONDS.sleep(CHECK_INTERVAL)
        catch {
          case e: InterruptedException =>
            logger.warn("interrupted", e)
            Thread.currentThread.interrupt()
        }
      }
    }
  }

  private def updateLogDetail(onlineServerNum: Int): Unit = {

    val logdateList = {
      val dao = new HiveSinkDetailDao(dbConnectURL, zookeeperServiceName)
      try {
        dao.connect()
        val list = dao.getFinishedLogdateList(onlineServerNum)
        if (list.nonEmpty) dao.updateCheckedState(list)
        list
      } catch {
        case e: SQLException =>
          logger.error(CommonUtils.getStackTraceStr(e))
          Nil
      } finally if (dao != null) dao.close()
    }

    if (logdateList != null && logdateList.nonEmpty) { // TODO DTE should have logDetail batch update API for better performance
      for (logData <- logdateList) {
        DTEUtils.updateLogDetail(updateLogDetailURL, logId, logData)
      }
      logger.info("Update DTE LogDetail, logid: " + logId + ", logdateList: " + logdateList)
    }
  }

  def configure(context: Context): Unit = {
    conf = new Configuration()
    dbName = context.getString(Config.HIVE_DATABASE, Config.Default.DEFAULT_DATABASE)
    tableName = Preconditions.checkNotNull(context.getString(Config.HIVE_TABLE), Config.HIVE_TABLE + " is required", null)
    path = Preconditions.checkNotNull(context.getString(Config.HIVE_PATH), Config.HIVE_PATH + " is required", null)
    partition = context.getString(Config.HIVE_PARTITION, Config.Default.DEFAULT_PARTITION)
    fileName = context.getString(Config.HIVE_FILE_PREFIX, Config.Default.DEFAULT_FILE_PREFIX)
    suffix = context.getString(Config.HIVE_FILE_SUFFIX, Config.Default.DEFAULT_FILE_SUFFIX)
    val tzName = context.getString(Config.HIVE_TIME_ZONE)
    timeZone = if (tzName == null) null else TimeZone.getTimeZone(tzName)
    maxOpenFiles = context.getInteger(Config.HIVE_MAX_OPEN_FILES, Config.Default.DEFAULT_MAX_OPEN_FILES)
    batchSize = context.getInteger(Config.HIVE_BATCH_SIZE, Config.Default.DEFAULT_BATCH_SIZE)
    idleTimeout = context.getLong(Config.HIVE_IDLE_TIMEOUT, Config.Default.DEFAULT_IDLE_TIMEOUT)
    idleQueueSize = context.getInteger(Config.HIVE_IDLE_QUEUE_SIZE, Config.Default.DEFAULT_IDLE_QUEUE_SZIE)
    idleWriterCloseThreadPoolSize = context.getInteger(Config.HIVE_IDLE_CLOSE_THREAD_POOL_SIZE, Config.Default.DEFAULT_IDLE_CLOSE_THREAD_POOL_SIZE)
    val serdeName = Preconditions.checkNotNull(context.getString(Config.HIVE_SERDE), Config.HIVE_SERDE + " is required", null)
    val serdeProperties = context.getSubProperties(Config.HIVE_SERDE_PROPERTIES + ".")
    try {
      val tbl = HiveUtils.getTableColunmnProperties(dbName, tableName)
      import scala.collection.JavaConversions._
      for (entry <- serdeProperties.entrySet) {
        tbl.setProperty(entry.getKey, entry.getValue)
      }
      deserializer = Class.forName(serdeName).newInstance.asInstanceOf[Nothing]
      deserializer.initialize(conf, tbl)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(serdeName + " init failed", e)
    }
    needRounding = context.getBoolean(Config.HIVE_ROUND, Config.Default.DEFAULT_ROUND)
    if (needRounding) {
      val unit = context.getString(Config.HIVE_ROUND_UNIT, Config.Default.DEFAULT_ROUND_UNIT)
      if (unit.equalsIgnoreCase("hour")) this.roundUnit = Calendar.HOUR_OF_DAY
      else if (unit.equalsIgnoreCase("minute")) this.roundUnit = Calendar.MINUTE
      else if (unit.equalsIgnoreCase("second")) this.roundUnit = Calendar.SECOND
      else {
        logger.warn("Rounding unit is not valid, please set one of " + "minute, hour, or second. Rounding will be disabled")
        needRounding = false
      }
      this.roundValue = context.getInteger(Config.HIVE_ROUND_VALUE, Config.Default.DEFAULT_ROUND_VALUE)
      if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) Preconditions.checkArgument(roundValue > 0 && roundValue <= 60, "Round value must be > 0 and <= 60", null)
      else if (roundUnit == Calendar.HOUR_OF_DAY) Preconditions.checkArgument(roundValue > 0 && roundValue <= 24, "Round value must be > 0 and <= 24", null)
    }
    this.useLocalTime = context.getBoolean(Config.HIVE_USE_LOCAL_TIMESTAMP, Config.Default.DEFAULT_USE_LOCAL_TIMESTAMP)
    if (sinkCounter == null) {
      sinkCounterType = context.getString(Config.HIVE_SINK_COUNTER_TYPE, Config.Default.DEFAULT_SINK_COUNTER_TYPE)
      if (sinkCounterType == "TimedSinkCounter") {
        sinkCounter = new TimedSinkCounter(getName)
        timedSinkCounterCategoryKey = context.getString("timedSinkCounterCategoryKey", "category")
      }
      else sinkCounter = new SinkCounter(getName)
    }
    this.zookeeperConnect = context.getString(Config.Hive_ZOOKEEPER_CONNECT, Config.Default.DEFAULT_ZOOKEEPER_CONNECT)
    this.zookeeperSessionTimeout = context.getInteger(Config.HIVE_ZOOKEEPER_SESSION_TIMEOUT, Config.Default.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT)
    if (this.zookeeperConnect != null) {
      this.zookeeperServiceName = context.getString(Config.HIVE_ZOOKEEPER_SERVICE_NAME)
      if (this.zookeeperServiceName == null) this.zookeeperServiceName = this.dbName + "." + this.tableName
      this.hostName = Preconditions.checkNotNull(context.getString(Config.HIVE_HOST_NAME), Config.HIVE_HOST_NAME + " is required", null)
      this.dbConnectURL = context.getString(Config.HIVE_DB_CONNECT_URL, Config.Default.DEFAULT_DB_CONNECT_URL)
      if (this.dbConnectURL != null) {
        this.updateLogDetailURL = Preconditions.checkNotNull(context.getString(Config.HIVE_DTE_UPDATE_LOGDETAIL_URL), Config.HIVE_DTE_UPDATE_LOGDETAIL_URL + " is required", null)
        this.logId = Preconditions.checkNotNull(context.getInteger(Config.HIVE_DTE_LOGID), s"${Config.HIVE_DTE_LOGID} is required", null).intValue()
        this.logdateFormat = context.getString(Config.HIVE_DTE_LOGDATE_FORMAT, Config.Default.DEFAULT_DTE_LOGDATE_FORMAT)
      }
    }
  }

  @throws[EventDeliveryException]
  override def process: Sink.Status = {
    val channel = getChannel
    val transaction = channel.getTransaction
    // TODO no need to store all the events content in array list, which will increase memory usage
    val events = new mutable.ListBuffer[Event]
    transaction.begin
    try {
      var txnEventCount = 0
      while ( {
        txnEventCount < batchSize
      }) {
        val event = channel.take
        if (event != null) {

          val rootPath = BucketPath.escapeString(path, event.getHeaders, timeZone, needRounding, roundUnit, roundValue, useLocalTime)
          val realPartition = BucketPath.escapeString(partition, event.getHeaders, timeZone, needRounding, roundUnit, roundValue, useLocalTime)
          val realName = BucketPath.escapeString(fileName, event.getHeaders, timeZone, needRounding, roundUnit, roundValue, useLocalTime)
          val partitionPath = rootPath + HiveBatchedSink.DIRECTORY_DELIMITER + realPartition
          val keyPath = partitionPath + HiveBatchedSink.DIRECTORY_DELIMITER + realName
          synchronized {
            var writer = activeWriters.get(keyPath)
            if (writer == null) {
              val counter = writerCounter.addAndGet(1)
              val fullFileName = realName + "." + System.nanoTime + "." + counter + "." + this.suffix
              writer = initializeHiveBatchWriter(partitionPath, fullFileName, realPartition)
              activeWriters.put(keyPath, writer)
            }
            writer.append(event.getBody)
          }
          events += event
        }
        txnEventCount += 1
      }

      if (txnEventCount == 0) sinkCounter.incrementBatchEmptyCount
      else if (txnEventCount == batchSize) sinkCounter.incrementBatchCompleteCount
      else sinkCounter.incrementBatchUnderflowCount

      // FIXME data may not flush to orcfile after commit transaction, which will cause data lose
      transaction.commit

      if (txnEventCount < 1)
        Status.BACKOFF
      else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount)
        if (sinkCounterType == "TimedSinkCounter") {
          sinkCounter.asInstanceOf[TimedSinkCounter].addToEventDrainSuccessCountInFiveMinMap(txnEventCount)
          sinkCounter.asInstanceOf[TimedSinkCounter].addToCategoryEventDrainSuccessCountInFiveMinMap(events.toList, timedSinkCounterCategoryKey)
        }
        Status.READY
      }
    } catch {
      case e: IOException =>
        transaction.rollback
        logger.warn("Hive IO error", e)
        Status.BACKOFF
      case e: Exception =>
        transaction.rollback
        logger.error("process failed", e)
        throw new EventDeliveryException(e)
    } finally transaction.close
  }

  @throws[IOException]
  @throws[SerDeException]
  private def initializeHiveBatchWriter(path: String, fileName: String, partition: String) = {
    val file = path + HiveBatchedSink.DIRECTORY_DELIMITER + fileName
    val logdate = HiveUtils.getPartitionValue(partition, "logdate")
    val values = HiveUtils.getPartitionValues(partition)
    val closeCallbacks = new mutable.ListBuffer[Callback]
    val addPartitionCallback = new AddPartitionCallback(dbName, tableName, values, path)

    closeCallbacks += addPartitionCallback
    if (this.dbConnectURL != null && logdate != null) {
      val updateSinkDetailCallback = new UpdateSinkDetailCallback(this.dbConnectURL, this.zookeeperServiceName, logdate, this.hostName, this.sinkCounter)
      closeCallbacks+= updateSinkDetailCallback
    }
    val writer = new HiveBatchedWriter(conf, deserializer, file)
    writer.setIdleTimeout(idleTimeout)
    writer.setCloseCallbacks(closeCallbacks.toList)
    writer.setLogdate(logdate)
    writer.setLogdateFormat(logdateFormat)
    if (logdate != null && logdateFormat != null) try {
      val minFinishedTimestamp = CommonUtils.convertTimeStringToTimestamp(logdate, logdateFormat) + CommonUtils.getMillisecond(roundValue, roundUnit)
      writer.setMinFinishedTimestamp(minFinishedTimestamp)
    } catch {
      case e: ParseException =>
        logger.error(CommonUtils.getStackTraceStr(e))
    }
    writer
  }

  override def start(): Unit = {
    this.isRunning = true
    this.writerCounter = new AtomicLong(0)
    this.activeWriters = new WriterLinkedHashMap(maxOpenFiles)
    this.idleWriters = new ArrayBlockingQueue[HiveBatchedWriter](idleQueueSize, true)
    this.idleWriterCloseThreadPool = Executors.newFixedThreadPool(idleWriterCloseThreadPoolSize, new ThreadFactoryBuilder().setNameFormat("idleWriterCloseThread-%d").build)
    var i = 0
    while ( {
      i < idleWriterCloseThreadPoolSize
    }) {
      idleWriterCloseThreadPool.submit(new IdleWriterCloseThread)

      {
        i += 1;
        i - 1
      }
    }
    this.idleWriterRemoveThread = new  IdleWriterRemoveThread
    new Thread(this.idleWriterRemoveThread, "IdleWriterCleanThread").start()
    sinkCounter.start()
    if (this.zookeeperConnect != null) {
      this.zkService = new ZKService(this.zookeeperConnect, this.zookeeperServiceName, this.hostName, this.zookeeperSessionTimeout)
      try
        this.zkService.start()
      catch {
        case e: ZKServiceException =>
          logger.error("Fail to start ZKService", e)
      }
      if (this.dbConnectURL != null) { // To Update DTE LogDetail
        this.leaderThread = new LeaderThread
        new Thread(this.leaderThread, "HiveBatchSinkLeaderThread").start()
      }
    }
    super.start()
  }

  override def stop(): Unit = {
    val iterator = activeWriters.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      logger.info(s"Closing ${entry.getKey}")
      try {
        entry.getValue.close()
      }
      catch {
        case e: Exception =>
          logger.warn(s"Exception while closing ${entry.getKey}. Exception follows.", e)
      }

    }
    activeWriters.clear()
    activeWriters = null
    sinkCounter.stop()
    if (this.zkService != null) try
      this.zkService.stop()
    catch {
      case e: ZKServiceException =>
        logger.error("Fail to stop ZKService", e)
    }
    this.isRunning = false
    super.stop()
  }

  override def toString: String = s"{ Sink type:${getClass.getSimpleName}, name:$getName }"
}
