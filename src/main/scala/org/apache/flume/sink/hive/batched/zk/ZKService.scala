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

package org.apache.flume.sink.hive.batched.zk

import java.io.IOException
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.flume.sink.hive.batched.util.CommonUtils
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper._

import scala.collection.JavaConversions._

object ZKService {

  private val ONLINE_PATH = "/online"


  private object ZKState extends Enumeration {
    type ZKState = Value
    val STOPPED, // set only when call close() or exceed max connect retry times
    RUNNING, EXPIRED = Value // set only when zk received Expired event = Value
  }

  trait ZKServiceOnStopCallBack {
    def cleanUp(): Unit
  }

}

case class ZKService(var zkURL: String, var serviceName: String, hostName: String, sessionTimeout: Int = 5000) extends LazyLogging {

  private var client: ZKClientWrapper = _
  private val currentServerInfo = ServerInfo(hostName, 0, null)
  private var state = ZKService.ZKState.STOPPED
  private val callBacks = new LinkedBlockingQueue[ZKService.ZKServiceOnStopCallBack]

  case class ServerInfo(hostName: String, var sessionId: Long, var sequenceId: String) {

    def getZNodePath: String = s"${hostName}_${sessionId}_$sequenceId"

    def getFullZnodePath: String = s"$getRootPath/$getZNodePath"

    override def toString: String = "ServerInfo{" + "hostName='" + hostName + '\'' + ", sessionId='" + sessionId + '\'' + ", sequenceId='" + sequenceId + '\'' + '}'
  }

  private class ZKClientWrapper() {

    private val watcher = new ZKWatcher()

    // a CountDownLatch initialized with 1 and used to wait zk connection
    private val connectedSignal = new CountDownLatch(1)

    val zk: ZooKeeper = {
      val conn = new ZooKeeper(zkURL, sessionTimeout, watcher)

      if (connectedSignal.await(10000, TimeUnit.MILLISECONDS)) {
        logger.info("Zookeeper connection create succeed.")
      }
      else {
        logger.error("Zookeeper connection create failed.")
        throw ZKServiceException("Fail to create zookeeper connection.")
      }
      conn
    }

    @throws[InterruptedException]
    def close(): Unit = {
      if (zk != null) {
        zk.close()
        logger.info("Zookeeper connection closed.")
      }
    }

    private class ZKWatcher extends Watcher {
      override def process(event: WatchedEvent): Unit = {
        logger.debug("Receive event: {}, {}", event.getType, event.getState)
        if (event.getState == Event.KeeperState.SyncConnected) connectedSignal.countDown()
        else if ((event.getState == Event.KeeperState.Expired) && isRunning) {
          synchronized {
            if (isRunning) { // Double check
              // if ZKService is STOPPED, no need to enter synchronized block
              // when enter synchronized block, if ZKService is STOPPED, no need to set EXPIRED
              state = ZKService.ZKState.EXPIRED
            }
          }

        }
      }
    }

  }

  private class ZKClientManagerThread extends Runnable {

    private val SLEEP_INTERVAL_SECONDS = 10
    private val THREAD_NAME = "ZKClientManagerThread"
    private val MAX_CONNECT_RETRY_TIMES = 20
    private var connectRetryTimes = 0

    override def run(): Unit = {

      Thread.currentThread.setName(THREAD_NAME)
      while (isRunning) {
        synchronized {
          if (isRunning && state == ZKService.ZKState.EXPIRED) {
            logger.info("ZooKeeper is expired.")
            try {
              offline()
              if (connectRetryTimes >= MAX_CONNECT_RETRY_TIMES) state = ZKService.ZKState.STOPPED
              else {
                connectRetryTimes += 1
                logger.info("Try to reconnect... (connectRetryTimes: {})", connectRetryTimes)
                online()
                state = ZKService.ZKState.RUNNING
                connectRetryTimes = 0
              }
            } catch {
              case e: Exception =>
                logger.error(CommonUtils.getStackTraceStr(e))
                if (client != null) try
                  client.close()
                catch {
                  case e1: InterruptedException =>
                    logger.error(CommonUtils.getStackTraceStr(e1))
                }
            }
          }
        }

        try
          TimeUnit.SECONDS.sleep(SLEEP_INTERVAL_SECONDS)
        catch {
          case e: InterruptedException =>
            logger.error(CommonUtils.getStackTraceStr(e))
        }
      }
      stopQuietly()
    }
  }


  @throws[ZKServiceException]
  def start(): Unit = {
    synchronized {
      if (isRunning) {
        logger.info("ZKService is already running.")
        return
      }
      try {
        online()
        state = ZKService.ZKState.RUNNING
        new Thread(new ZKClientManagerThread).start()
        logger.info("ZKService is started.")
      } catch {
        case e: Exception =>
          logger.error(CommonUtils.getStackTraceStr(e))
          stopQuietly()
          throw new ZKServiceException("Fail to start ZKService.", e)
      }
    }
  }

  @throws[KeeperException]
  @throws[InterruptedException]
  private def create(path: String, isChild: Boolean): Unit = {
    if (client.zk.exists(path, false) == null)
      client.zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        if (isChild) CreateMode.EPHEMERAL else CreateMode.PERSISTENT
      )
  }

  @throws[KeeperException]
  @throws[InterruptedException]
  private def prepareEnv(): Unit = {
    create("/" + this.serviceName, isChild = false)
    create(getRootPath, isChild = false)
  }

  @throws[KeeperException]
  @throws[InterruptedException]
  @throws[IOException]
  @throws[ZKServiceException]
  private def online(): Unit = {
    client = new ZKClientWrapper()
    prepareEnv()
    currentServerInfo.sessionId = client.zk.getSessionId
    val zNodeBasePath = s"$getRootPath/${currentServerInfo.hostName}_${currentServerInfo.sessionId}_"
    val path = client.zk.create(zNodeBasePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)
    val info = path.split("_")
    if (info.length == 3) currentServerInfo.sequenceId = info(2)
    client.zk.exists(currentServerInfo.getFullZnodePath, true)
    logger.info("Server {} online.", currentServerInfo.hostName)
  }

  @throws[KeeperException]
  @throws[InterruptedException]
  def getAllServerInfos: List[ServerInfo] = {
    // FIXME need to call sync() to get latest view
    client.zk
      .getChildren(getRootPath, false)
      .toList
      .map(path => {

        val info = path.split("_")
        if (info.length == 3) ServerInfo(info(0), info(1).toLong, info(2))
        else {
          logger.error("Invalid server znode path: " + path)
          null
        }
      }).filter(_ != null)
  }

  @throws[KeeperException]
  @throws[InterruptedException]
  def getLeader: ServerInfo = {
    val serverInfos = getAllServerInfos
    var leader: ServerInfo = null

    for (serverInfo <- serverInfos) {
      if (leader == null) leader = serverInfo
      else if (serverInfo.sequenceId.compareTo(leader.sequenceId) < 0) leader = serverInfo
    }
    leader
  }

  def isRunning: Boolean = state ne ZKService.ZKState.STOPPED

  @throws[ZKServiceException]
  def stop(): Unit = {
    this.synchronized {
      state = ZKService.ZKState.STOPPED
      try {
        offline()
        logger.info("ZKService is stopped.")
        executeAllCallBacks()
      } catch {
        case e: Exception =>
          logger.error(CommonUtils.getStackTraceStr(e))
          throw new ZKServiceException("Fail to stop ZKService.", e)
      }
    }
  }

  def stopQuietly(): Unit = {
    try
      stop()
    catch {
      case e: Exception =>
        logger.error(CommonUtils.getStackTraceStr(e))
    }
  }

  @throws[InterruptedException]
  def addCallBack(callBack: ZKService.ZKServiceOnStopCallBack): Unit = {
    callBacks.put(callBack)
  }

  @throws[InterruptedException]
  private def executeAllCallBacks(): Unit = {
    while ( {
      !callBacks.isEmpty
    }) {
      val callBack = callBacks.take
      try {
        logger.info("execute callback on zk stopped ...")
        callBack.cleanUp()
      } catch {
        case e: Exception =>
          logger.error(CommonUtils.getStackTraceStr(e))
      }
    }
  }

  @throws[InterruptedException]
  private def offline(): Unit = {
    if (client != null) client.close()
    logger.info("Server {} offline.", currentServerInfo.hostName)
  }

  private def getRootPath = "/" + this.serviceName + ZKService.ONLINE_PATH
}

