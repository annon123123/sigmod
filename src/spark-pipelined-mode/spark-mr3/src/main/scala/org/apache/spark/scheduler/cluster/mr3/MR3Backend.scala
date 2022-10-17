/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mr3

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import javax.annotation.concurrent.GuardedBy

import mr3.api.common.{MR3Conf, MR3ConfBuilder, MR3Constants, MR3Exception}
import mr3.DAGAPI
import mr3.api.client.DAGClient
import com.google.protobuf.ByteString
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.{BarrierCoordinator, ExceptionFailure, MapOutputTrackerMaster, Partition, SparkContext, SparkEnv, SparkException, TaskFailedReason, TaskState}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{BARRIER_SYNC_TIMEOUT, CPUS_PER_TASK, EXECUTOR_CORES}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StopDriver
import org.apache.spark.scheduler.cluster.ExecutorData
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ShutdownHookManager, ThreadUtils}

import scala.collection.{Map, mutable}
import scala.collection.JavaConverters._

private[mr3] object SparkMR3TaskID {

  // SparkTaskID: unique for every Spark Task
  // MR3TaskID == 'SparkTaskAttemptID': unique for every Spark Task + attemptNumber (similarly to TaskAttemptID in MR3)
  // *** note that MR3TaskID, not SparkTaskID, corresponds to 'taskId: Long' in Spark ***

  type SparkTaskID = Long
  type MR3TaskID = Long     // | SparkTaskID (56bit) | TaskAttemptID.id (8bit) |

  def toSparkTaskId(mr3TaskId: MR3TaskID): SparkTaskID = {
    mr3TaskId >> 8
  }

  def toMr3TaskId(sparkTaskId: SparkTaskID, taskAttemptIdId: Int): MR3TaskID = {
    assert { taskAttemptIdId < (1 << 8) }
    (sparkTaskId << 8) + taskAttemptIdId
  }

  def toTaskAttemptIdId(mr3TaskId: MR3TaskID): Int = {
    (mr3TaskId & 0xFF).toInt
  }

  def isTaskAttempt(sparkTaskId: SparkTaskID, mr3TaskId: MR3TaskID): Boolean = {
    sparkTaskId == toSparkTaskId(mr3TaskId)
  }
}

private[mr3] trait MR3BackendDriverEndpointInterface {
  import org.apache.spark.scheduler.cluster.mr3.SparkMR3TaskID.MR3TaskID

  def numExecutors: AtomicInteger
  def statusUpdate(
      executorId: String,
      hostname: String,
      taskId: MR3TaskID,
      state: TaskState,
      data: ByteBuffer): Unit
  def registerExecutor(executorId: String, hostname: String, data: ExecutorData): Unit
  def removeExecutor(executorId: String, executorLossReason: ExecutorLossReason): Unit
  def removeExecutorFromBlockManagerMaster(executorId: String): Unit
  def updateDelegationTokenManager(tokens: Array[Byte]): Unit
}

private[mr3] trait DAGStatusCheckerInterface {
  def dagStatusCheckerExecutor: ThreadPoolExecutor
  def sparkEnv: SparkEnv
  def notifyJobFailed(jobId: Int, failedStages: Seq[Int], taskFailedReason: TaskFailedReason): Unit
}

class MR3Backend(val sc: SparkContext) extends TaskScheduler
    with SchedulerBackend
    with MR3BackendDriverEndpointInterface
    with DAGStatusCheckerInterface
    with Logging {

  import SparkMR3TaskID.{MR3TaskID, SparkTaskID}

  def this(sc: SparkContext, masterURL: String) = this(sc)

  if (sc.conf.getOption(SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES).isEmpty) {
    logWarning(s"${SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES} is not defined. " +
      s"Adding the default value ${SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES_DEFAULT} to SparkConf")
    sc.conf.set(
      SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES,
      SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES_DEFAULT.toString)
  } else {
    require({sc.conf.get(SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES).toInt > 0},
      s"${SparkMR3Config.SPARK_MR3_PIPELINED_NUM_FETCHES} should be positive integer.")
  }

  def sparkEnv = sc.env
  def sparkConf = sc.conf   // immutable, so common to all DAGs
  private val ENDPOINT_NAME = "MR3Backend"

  // do not use System.currentTimeMillis because two Spark drivers sharing DAGAppMaster may be launched almost simultaneously
  private[this] val appId = "spark-mr3-app-" + java.util.UUID.randomUUID.toString.substring(0, 8)

  // from TaskScheduler
  override val schedulingMode: SchedulingMode = SchedulingMode.FIFO
  override val rootPool: Pool = new Pool("MR3 Pool", schedulingMode, 0, 0)

  // effectively immutable because setDAGScheduler() is called before submitTasks() is called
  private[mr3] var dagScheduler: DAGScheduler = _

  // accessed by DAGScheduler thread only, so no need to guard with a lock
  private[this] val taskSerializer = sc.env.closureSerializer.newInstance()
  private[this] var nextTaskId: SparkTaskID = 0L

  // lock for guarding taskSetManagersByTaskId[] and taskSetManagersByStageIdAttempt[]
  private[this] val taskSetManagersLock = new AnyRef

  // updated by DAGScheduler, MR3TaskResultGetter, and DAGStatusChecker threads
  // read by MR3DriverEndpoint and HeartbeatReceiver threads
  @GuardedBy("taskSetManagersLock")
  private[this] val taskSetManagersByTaskId = new mutable.HashMap[SparkTaskID, MR3TaskSetManager]
  assert { taskSetManagersByTaskId forall { case (sparkTaskId, tsm) => tsm.containSparkTaskId(sparkTaskId) } }

  // updated by DAGScheduler, MR3TaskResultGetter, and DAGStatusChecker threads
  // read by DAGScheduler thread
  @GuardedBy("taskSetManagersLock")
  private[this] val taskSetManagersByStageIdAttempt = new mutable.HashMap[(Int, Int), MR3TaskSetManager]

  // for taskSetManagersByTaskId and taskSetManagersByStageIdAttempt
  assert { taskSetManagersByStageIdAttempt.values forall taskSetManagersByTaskId.values.toSeq.contains }
  assert { taskSetManagersByTaskId.values forall taskSetManagersByStageIdAttempt.values.toSeq.contains }

  @GuardedBy("taskSetManagersLock")
  private[this] val jobIdToTaskSetManagers = new mutable.HashMap[Int, mutable.Set[MR3TaskSetManager]]

  // Any thread except DAGScheduler that stops and removes DAGStatusChecker should notify it to DAGScheduler.
  @GuardedBy("taskSetManagersLock")
  private[this] val jobIdToDagStatusChecker = new mutable.HashMap[Int, DAGStatusChecker]

  assert { jobIdToTaskSetManagers.keys forall jobIdToDagStatusChecker.keySet.contains }
  assert { jobIdToDagStatusChecker.keys forall jobIdToTaskSetManagers.keySet.contains }

  assert { jobIdToTaskSetManagers.values.forall { _.nonEmpty } }
  assert { jobIdToTaskSetManagers.valuesIterator.map { _.size }.sum ==
    jobIdToTaskSetManagers.values.flatten.toSeq.distinct.length }
  assert { jobIdToTaskSetManagers.values.flatten forall taskSetManagersByStageIdAttempt.values.toSeq.contains }
  assert { taskSetManagersByStageIdAttempt.values forall jobIdToTaskSetManagers.values.flatten.toSeq.contains }

  private def assertMR3Backend(): Unit = taskSetManagersLock.synchronized {
    assert { taskSetManagersByTaskId forall { case (sparkTaskId, tsm) => tsm.containSparkTaskId(sparkTaskId) } }
    assert { taskSetManagersByStageIdAttempt.values forall taskSetManagersByTaskId.values.toSeq.contains }
    assert { taskSetManagersByTaskId.values forall taskSetManagersByStageIdAttempt.values.toSeq.contains }
    assert { jobIdToTaskSetManagers.keys forall jobIdToDagStatusChecker.keySet.contains }
    assert { jobIdToDagStatusChecker.keys forall jobIdToTaskSetManagers.keySet.contains }
    assert { jobIdToTaskSetManagers.values.forall { _.nonEmpty } }
    assert { jobIdToTaskSetManagers.valuesIterator.map { _.size }.sum ==
      jobIdToTaskSetManagers.values.flatten.toSeq.distinct.length }
    assert { jobIdToTaskSetManagers.values.flatten forall taskSetManagersByStageIdAttempt.values.toSeq.contains }
    assert { taskSetManagersByStageIdAttempt.values forall jobIdToTaskSetManagers.values.flatten.toSeq.contains }
  }

  // updated by MR3DriverEndPoint
  val numExecutors = new AtomicInteger(0)
  private[this] val coresPerExecutor: Int = sparkConf.get(EXECUTOR_CORES)
  private[this] val cpusPerTask: Int = sparkConf.get(CPUS_PER_TASK)
  require({ coresPerExecutor >= cpusPerTask && cpusPerTask > 0 },
    s"Spark configuration invalid: $EXECUTOR_CORES = $coresPerExecutor, $CPUS_PER_TASK = $cpusPerTask")

  // TODO: blocking operation???
  private[this] val driverEndpoint = sc.env.rpcEnv.setupEndpoint(
      ENDPOINT_NAME, new MR3DriverEndpoint(sc.env.rpcEnv, backend = this))

  private[this] val taskResultGetter = new MR3TaskResultGetter(
      sc.env, sparkConf.getInt("spark.resultGetter.threads", 4))

  private[this] val credentials = new AtomicReference[Credentials]

  private[this] var delegationTokenManager: HadoopDelegationTokenManager = _

  private[this] var barrierCoordinator: BarrierCoordinator = _

  private[this] val mr3Conf = {
    val builder = new MR3ConfBuilder(true)
      .addResource(sc.hadoopConfiguration)
      .set(MR3Conf.MR3_RUNTIME, "spark")
      .setBoolean(MR3Conf.MR3_AM_SESSION_MODE, true)
      .set(MR3Conf.MR3_CONTAINER_MAX_JAVA_HEAP_FRACTION, Utils.getContainerHeapFraction(sparkConf).toString)
    sparkConf.getAll.foreach { case (key, value) =>
      if (key.startsWith(SparkMR3Config.SPARK_MR3_CONFIG_PREFIX) &&
          !SparkMR3Config.RESERVED_CONFIG_KEYS.contains(key)) {
        builder.set(key.replace("spark.", ""), value)
      }
    }
    val containerLaunchEnv = sparkConf.getExecutorEnv.map{ case (key, value) => key + "=" + value }.mkString(",")
    if (containerLaunchEnv.nonEmpty) {
      builder.set(MR3Conf.MR3_CONTAINER_LAUNCH_ENV, containerLaunchEnv)
    }
    builder.build
  }
  // In local worker mode, only a single ContainerWorker/executor should be created.
  require({ !(mr3Conf.getAmWorkerMode == MR3Constants.MR3_WORKER_MODE_LOCAL) ||
    mr3Conf.getAmLocalResourceSchedulerCpuCores == Utils.getContainerGroupCores(sparkConf) },
    s"In local worker mode, ${MR3Conf.MR3_AM_LOCAL_RESOURCESCHEDULER_CPU_CORES} must match ${SparkLauncher.EXECUTOR_CORES}.")
  require({ !(mr3Conf.getAmWorkerMode == MR3Constants.MR3_WORKER_MODE_LOCAL) ||
    mr3Conf.getAmLocalResourceSchedulerMaxMemoryMb == Utils.getContainerGroupNonNativeMemoryMb(sparkConf) },
    s"In local worker mode, ${MR3Conf.MR3_AM_LOCAL_RESOURCESCHEDULER_MAX_MEMORY_MB} must match " +
    s"${SparkLauncher.EXECUTOR_MEMORY} + spark.executor.memoryOverhead")

  private[this] val sparkMr3Client = {
    val rpcAddress = driverEndpoint.address
    val driverName = ENDPOINT_NAME
    val driverAddress = RpcEndpointAddress(rpcAddress, driverName)
    val driverUrl = driverAddress.toString
    val ioEncryptionKey = sc.env.securityManager.getIOEncryptionKey()
    new SparkMR3Client(mr3Conf, sparkConf, sparkApplicationId = applicationId, driverUrl = driverUrl, ioEncryptionKey)
  }

  val dagStatusCheckerPeriodMs: Int = sparkConf.getInt(
      SparkMR3Config.SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS,
      SparkMR3Config.SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS_DEFAULT)

  //
  // DAGStatusCheckerInterface
  //

  val dagStatusCheckerExecutor: ThreadPoolExecutor = ThreadUtils.newDaemonCachedThreadPool("DAGStatusChecker")

  def notifyJobFailed(jobId: Int, failedStages: Seq[Int], taskFailedReason: TaskFailedReason): Unit = {
    val message = s"DAG failed/killed, most recent failure: ${taskFailedReason.toErrorString}"
    val exception = taskFailedReason match {
      case ef: ExceptionFailure => ef.exception
      case _ => None
    }

    jobIdToTaskSetManagers(jobId)
      .filter { tsm => failedStages.contains(tsm.stageId) }
      .foreach { tsm =>
        val taskSet = tsm.taskSet
        dagScheduler.taskSetFailed(taskSet, message, exception)
      }

    unregisterJob(jobId)
    dagScheduler.cancelJob(jobId, Some(s"The corresponding MR3 DAG has been failed."))
  }

  //
  // SchedulerBackend, TskScheduler
  //

  @throws[InterruptedException]
  @throws[SparkException]
  def start(): Unit = {
    ShutdownHookManager.addShutdownHook { () =>
      logInfo(s"MR3Backend shutting down: $appId")
      taskSetManagersLock.synchronized {
        // dagStatueChecker.stop() just sets shouldKillDag, and the DAGStatusChecker thread may not have to
        // chance to read it. Hence we use dagStatueChecker.kill() to invoke DAGClient.tryKillDag().
        // Note that DAGClient.getDagStatusWait() is currently in progress in DAGStatusChecker.
        jobIdToDagStatusChecker.values foreach { _.kill() }
      }
    }
    startHadoopDelegationTokenManager()
    sparkMr3Client.start()
  }

  private def startHadoopDelegationTokenManager(): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      delegationTokenManager = new HadoopDelegationTokenManager(sc.conf, sc.hadoopConfiguration, driverEndpoint)
      val tokens =
        if (delegationTokenManager.renewalEnabled) {
          delegationTokenManager.start()
        } else {
          val creds = UserGroupInformation.getCurrentUser.getCredentials
          delegationTokenManager.obtainDelegationTokens(creds)
          if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
            SparkHadoopUtil.get.serialize(creds)
          } else
            null
        }
      if (tokens != null) {
        updateDelegationTokenManager(tokens)
      }
    }
  }

  @throws[SparkException]
  def stop(): Unit = {
    logInfo(s"MR3Backend stopping: $appId")

    taskSetManagersLock.synchronized {
      taskSetManagersByStageIdAttempt.values foreach { _.kill("MR3Backend.stop() called") }
      // The following assert{} is invalid because unregisterTaskSetManager() may be called from
      // MR3TaskSetManager.taskSucceeded() with transitionToSucceeded == true.
      //   assert { taskSetManagersByStageIdAttempt.isEmpty }
      while (taskSetManagersByStageIdAttempt.nonEmpty) {
        taskSetManagersLock.wait()
      }
    }

    taskResultGetter.stop()
    sparkMr3Client.stop()
    dagStatusCheckerExecutor.shutdownNow()
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    if (delegationTokenManager != null) {
      delegationTokenManager.stop()
    }
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askSync[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping MR3DriverEndpoint", e)
    }
  }

  // return the default level of parallelism to use in the cluster, as a hint for sizing jobs
  // Cf. org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
  def defaultParallelism(): Int = {
    val maxNumTasks = maxNumConcurrentTasks() max 2
    sparkConf.getInt("spark.default.parallelism", maxNumTasks)
  }

  override def applicationId: String = appId

  //
  // SchedulerBackend
  //

  def reviveOffers(): Unit = {
    // In Spark on MR3, Spark delegates scheduling decision to MR3.
    // So, this method does nothing and we should not call it.
    assert { false }
  }

  // We don't support ResourceProfile as of now,
  // so we assume that all executors have the same ResourceProfile(ID).
  def maxNumConcurrentTasks(rp: ResourceProfile): Int = maxNumConcurrentTasks()

  // return the max number of tasks that can be concurrently launched
  // return 0 if no executors are running (Cf. CoarseGrainedSchedulerBackend.scala in Spark)
  private def maxNumConcurrentTasks(): Int = {
    val currentNumExecutors = numExecutors.get()
    val numTasksPerExecutor = coresPerExecutor / cpusPerTask
    numTasksPerExecutor * currentNumExecutors
  }

  //
  // TaskScheduler
  //

  // Called by DAGScheduler thread
  // This may throw an exception but The caller method DAGScheduler.submitActiveJob() will handle it.
  override def submitJob(
      job: ActiveJob,
      stages: Map[Int, Stage],
      taskBinaries: Map[Int, Broadcast[Array[Byte]]],
      taskMetrics: Map[Int, Array[Byte]],
      properties: Map[Int, Properties],
      stageIdToPartitions: Map[Int, Array[Partition]],
      partitionsToCompute: Map[Int, Seq[Int]],
      stageIdToPreferredLocations: Map[Int, Map[Int, Seq[TaskLocation]]]): Unit = {
    val epoch = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].getEpoch
    val addedFiles = mutable.HashMap[String, Long](sc.addedFiles.toSeq: _*)
    val addedArchives = mutable.HashMap[String, Long](sc.addedArchives.toSeq: _*)
    val addedJars = mutable.HashMap[String, Long](sc.addedJars.toSeq: _*)

    val firstSparkTaskIdOfStage = new mutable.HashMap[Int, SparkTaskID]
    stages.values.foreach { stage =>
      val numTasks = partitionsToCompute(stage.id).size
      firstSparkTaskIdOfStage(stage.id) = nextTaskId
      nextTaskId += numTasks

      if (stage.rdd.isBarrier()) {
        initializeBarrierCoordinatorIfNecessary()
        // BarrierTaskContext requires the list of addresses of all tasks in the same stage, ordered by partition ID.
        // However, Spark uses this information only to check if all tasks have been launched.
        // Thus using fake addresses is okay for Spark-MR3, which does not fully support barrier jobs.
        // Note that the fake addresses can be obtained via BarrierTaskContext.getTaskInfos().
        val taskIds = (0 until numTasks).mkString(",")
        properties(stage.id).setProperty("addresses", taskIds)
      }
    }

    def createFileTimeStampProto(url: String, timestamp: Long): DAGAPI.FileTimeStampProto.Builder = {
      DAGAPI.FileTimeStampProto.newBuilder
        .setUrl(url)
        .setTimestamp(timestamp)
    }
    val addedFileProtoList = addedFiles.map { case (url, ts) => createFileTimeStampProto(url, ts) }
    val addedArchiveProtoList = addedArchives.map { case (url, ts) => createFileTimeStampProto(url, ts) }
    val addedJarProtoList = addedJars.map { case (url, ts) => createFileTimeStampProto(url, ts) }

    val stageInfoProtoMap = new mutable.HashMap[Int, DAGAPI.SparkStageInfoProto.Builder]
    stages.values.foreach { stage =>
      val stageId = stage.id
      val taskBinary = taskBinaries(stageId)
      val serializedTaskBinary = ByteString.copyFrom(taskSerializer.serialize(taskBinary))

      val stageInfoProto = DAGAPI.SparkStageInfoProto.newBuilder
        .setEpoch(epoch)
        .setFirstTaskId(firstSparkTaskIdOfStage(stageId))
        .setSerializedTask(serializedTaskBinary)
        .setSerializedTaskMetric(ByteString.copyFrom(taskMetrics(stageId)))
        .setStageId(stageId)
        .setStageAttempt(stage.latestInfo.attemptNumber())
        .setApplicationId(sc.applicationId)
        .setJobId(job.jobId)
        .setIsBarrier(stage.rdd.isBarrier())

      addedFileProtoList.foreach { stageInfoProto.addAddedFiles }
      addedArchiveProtoList.foreach { stageInfoProto.addAddedArchives }
      addedJarProtoList.foreach { stageInfoProto.addAddedJars }
      properties(stageId).asScala.foreach { case (key, value) =>
        val keyValueProto = DAGAPI.KeyValueProto.newBuilder
          .setKey(key)
          .setValue(value)
        stageInfoProto.addProperties(keyValueProto)
      }
      sc.applicationAttemptId.foreach(stageInfoProto.setApplicationAttempt)

      stageInfoProtoMap.put(stage.id, stageInfoProto)
    }

    def convertTaskLocationHint(preferredLocations: Seq[TaskLocation]): Seq[String] = {
      preferredLocations.flatMap {
        case e: ExecutorCacheTaskLocation =>
          // Cf. SparkRuntimeEnv.createExecutorBackend()
          // In LocalThreadMaster-LocalWorker mode, executorId includes "driver" instead of '#'
          // because MR3ExecutorBackend uses the existing SparkEnv created by driver.
          assert { e.executorId.contains('#') || e.executorId.contains("driver") }
          Seq(e.executorId.split('#').head, e.host)
        case h: HostTaskLocation =>
          Some(h.host)
        case h: HDFSCacheTaskLocation =>
          // TODO: for HDFSCacheTaskLocation, first add all executorId's on it and then add its host
          Some(h.host)
      }
    }

    val partitionAndTaskLocationMap =
      new mutable.HashMap[Int, Seq[(DAGAPI.SparkPartitionInfoProto.Builder, Seq[String])]]
    stages.values.foreach { stage =>
      val stageId = stage.id
      val partitionIds = partitionsToCompute(stageId)
      val partitions = stageIdToPartitions(stageId)
      val preferredLocations = stageIdToPreferredLocations(stageId)

      // TaskIndex: the index of Task in a Vertex(TaskSet)
      // PartitionId: the index of partition in an RDD
      // OutputId: (for ResultTask only) the index of partition in requested partitions to be computed

      val taskInfoList = stage match {
        case _: ShuffleMapStage =>
          partitionIds.map { partitionId =>
            val serializedPartition = taskSerializer.serialize(partitions(partitionId))
            val partitionInfoProto = DAGAPI.SparkPartitionInfoProto.newBuilder
              .setSerializedPartition(ByteString.copyFrom(serializedPartition))
              .setPartitionId(partitionId)
            val taskLocationHint = convertTaskLocationHint(preferredLocations(partitionId))
            (partitionInfoProto, taskLocationHint)
          }
        case rs: ResultStage =>
          partitionIds.map { outputId =>
            val partitionId = rs.partitions(outputId)
            val serializedPartition = taskSerializer.serialize(partitions(partitionId))
            val partitionInfoProto = DAGAPI.SparkPartitionInfoProto.newBuilder
              .setSerializedPartition(ByteString.copyFrom(serializedPartition))
              .setPartitionId(partitionId)
              .setResultOutputId(outputId)
            val taskLocationHint = convertTaskLocationHint(preferredLocations(outputId))
            (partitionInfoProto, taskLocationHint)
          }
      }

      partitionAndTaskLocationMap.put(stageId, taskInfoList)
    }

    val stageMetaDataMap = stages.values.map { stage =>
      val stageId = stage.id
      val missingPartitions = partitionsToCompute(stageId)

      val numTasks = missingPartitions.length
      val numPartitions = stage.numPartitions
      val parentStageIds = stage.parents.map(_.id).filter(stages.contains)

      val shuffleIdOpt = stage match {
        case sms: ShuffleMapStage => Some(sms.shuffleDep.shuffleId)
        case _ => None
      }

      val stageMetaData = SparkStageMetaData(shuffleIdOpt, numTasks = numTasks, numPartitions = numPartitions,
        parentStageIds = parentStageIds)
      stageId -> stageMetaData
    }.toMap

    val partiallyComputedStageIds = stageMetaDataMap.filter { case (_, stageMetaData) =>
      stageMetaData.numTasks != stageMetaData.numPartitions
    }.keys.toSeq
    stages.values.foreach { stage =>
      require({ stage.parents.forall { p => !partiallyComputedStageIds.contains(p.id) } },
        "Partially computed stage cannot be parent of other stages.")
    }

    val taskSets = stages.values.map { stage =>
      val stageId = stage.id
      val stageAttemptNumber = stage.latestInfo.attemptNumber()
      val jobId = job.jobId

      // We should create Tasks because CompletionEvent requires a Task.
      val tasks: Seq[Task[_]] = stage match {
        case sms: ShuffleMapStage =>
          partitionsToCompute(stageId).map { partitionId =>
            new ShuffleMapTask(stageId = stageId, stageAttemptId = stageAttemptNumber,
                taskBinaries(stageId), stageIdToPartitions(stageId)(partitionId),
                stageIdToPreferredLocations(stageId)(partitionId), properties(stageId), taskMetrics(stageId),
                Option(jobId), appId = Option(sc.applicationId), appAttemptId = sc.applicationAttemptId,
                stage.rdd.isBarrier()
            )
          }
        case rs: ResultStage =>
          partitionsToCompute(stageId).map { outputId =>
            val partitionId = rs.partitions(outputId)
            new ResultTask(stageId = stageId, stageAttemptId = stageAttemptNumber,
                taskBinaries(stageId), stageIdToPartitions(stageId)(partitionId),
                stageIdToPreferredLocations(stageId)(outputId), outputId = outputId, properties(stageId),
                taskMetrics(stageId), Option(jobId), appId = Option(sc.applicationId),
                appAttemptId = sc.applicationAttemptId, stage.rdd.isBarrier()
            )
          }
      }

      new TaskSet(tasks.toArray, stageId = stageId, stageAttemptId = stageAttemptNumber,
          priority = job.jobId, properties(stageId), resourceProfileId = stage.resourceProfileId)
    }.toList

    val dagConf = getMr3ConfProto(job.properties)

    try {
      taskSetManagersLock.synchronized {
        // MR3DriverEndpoint can call statusUpdate() right after we submit the DAG.
        // So stay inside taskSetManagersLock.synchronized{} until registerTaskSetManager() returns.
        val dagClient =
          sparkMr3Client.submitJob(
            job.jobId,
            stageMetaDataMap,
            stageInfoProtoMap,
            partitionAndTaskLocationMap,
            sparkConf,
            dagConf,
            Option(credentials.get))
        // sparkMr3Client.mr3Client is MR3SessionClient, and we have dagClient.ownDagClientHandler == false.
        // hence dagClient.close() should NOT be called.

        val taskSetManagers = taskSets.map { taskSet =>
          new MR3TaskSetManager(taskSet, firstSparkTaskIdOfStage(taskSet.stageId), job.jobId, backend = this)
        }
        registerTaskSetManagers(job.jobId, dagClient, taskSetManagers)
      }
    } catch {
      case ex: MR3Exception =>
        throw new SparkException(s"Fail to submit job (${job.jobId})", ex)
    }
  }

  override def killJob(jobId: Int): Unit = {
    logInfo(s"MR3Backend attempts to stop Job $jobId")
    unregisterJob(jobId)
  }

  // Spark-MR3 with pipelined shuffle does not call this method.
  def submitTasks(taskSet: TaskSet): Unit = {
    logError(s"Spark-MR3 does not support running a single stage from v3.2.1.")
    throw new UnsupportedOperationException
  }

  private def getMr3ConfProto(properties: Properties): DAGAPI.ConfigurationProto.Builder = {
    val mr3ConfProto = DAGAPI.ConfigurationProto.newBuilder
    val enum = properties.propertyNames
    while (enum.hasMoreElements) {
      val elem = enum.nextElement()
      elem match {
        case key: String =>
          if (key.startsWith(SparkMR3Config.SPARK_MR3_CONFIG_PREFIX) &&
              !SparkMR3Config.RESERVED_CONFIG_KEYS.contains(key)) {
            Option(properties.getProperty(key)) foreach { value =>
              val mr3ConfKey = key.replaceFirst(SparkMR3Config.SPARK_MR3_CONFIG_PREFIX, "mr3.")
              logInfo(s"Converting to add to DAGConf: $mr3ConfKey, $value")
              val keyValueProto = DAGAPI.KeyValueProto.newBuilder
                .setKey(mr3ConfKey)
                .setValue(value)
              mr3ConfProto.addConfKeyValues(keyValueProto)
            }
          }
        case _ =>
      }
    }
    mr3ConfProto
  }

  // This method is called by DAGScheduler thread only.
  // So it is safe to check the nullity of barrierCoordinator without synchronization.
  private def initializeBarrierCoordinatorIfNecessary(): Unit = {
    if (barrierCoordinator == null) {
      val barrierSyncTimeout = sparkConf.get(BARRIER_SYNC_TIMEOUT)
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus, sc.env.rpcEnv)
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
  }

  // Note. for Spark-MR3 without pipelined shuffle
  // Spark Executor refers to interruptThread when it creates TaskReaper to kill Task.
  // In Spark-MR3, MR3 provides only one method, named tryKillDag(), to stop Spark Task.
  // So we ignore interruptThread in cancelTasks/killAllTaskAttempts()
  // and treat interruptThread as true in Spark runtime.

  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = taskSetManagersLock.synchronized {
    // In pipelined shuffle, DAGScheduler should call killJob().
    logError(s"MR3Backend.cancelTasks() is called.")
    throw new UnsupportedOperationException
  }

  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    throw new UnsupportedOperationException
  }

  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit = {
    // In pipelined shuffle, DAGScheduler should call killJob().
    logError(s"MR3Backend.killAllTaskAttempts() is called.")
    throw new UnsupportedOperationException
  }

  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
    // In vanilla Spark, notifyPartitionCompletion() causes TaskSetManager to skip executing the Task with the same partitionId.
    // In Spark on MR3, TaskSetManager cannot stop individual tasks that are already submitted to MR3.
    // Hence notifyPartitionCompletion() is no-op.
  }

  // guaranteed to be called before submitTasks() is called
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    // cf. TaskSchedulerImpl
    // accumUpdatesWithTaskIds: Array[(taskId, stageId, stageAttemptId, accumUpdates)]
    val accumUpdatesWithTaskIds = taskSetManagersLock.synchronized {
      accumUpdates.flatMap { case (mr3TaskId, updates) =>
        val accumInfos = updates map { acc => acc.toInfo(Some(acc.value), None) }
        val sparkTaskId = SparkMR3TaskID.toSparkTaskId(mr3TaskId)
        taskSetManagersByTaskId.get(sparkTaskId)
          .map{ taskSetMgr => (mr3TaskId, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accumInfos) }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId, executorUpdates)
  }

  def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    throw new UnsupportedOperationException
  }

  def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logError("Running MR3Backend with Spark Standalone Cluster is denied.")
    assert { false }
  }

  def executorDecommission(executorId:  String, decommissionInfo:  ExecutorDecommissionInfo): Unit = {
    logError("executorDecommission() cannot be called in Spark-MR3.")
    assert { false }
  }

  def getExecutorDecommissionState(executorId: String): Option[ExecutorDecommissionState] = None

  //
  // MR3BackendDriverEndpointInterface
  //

  def statusUpdate(
      executorId: String,
      hostname: String,
      mr3TaskId: MR3TaskID,
      state: TaskState,
      data: ByteBuffer): Unit = {
    try {
      val sparkTaskId = SparkMR3TaskID.toSparkTaskId(mr3TaskId)
      val maybeTaskSetManager = taskSetManagersLock.synchronized { taskSetManagersByTaskId.get(sparkTaskId) }
      maybeTaskSetManager match {
        case Some(taskSetMgr) =>
          state match {
            case TaskState.RUNNING =>
              taskSetMgr.taskStarted(mr3TaskId, executorId, hostname)
            case TaskState.FINISHED =>
              taskResultGetter.fetchSuccessfulTask(
                  taskSetMgr, mr3TaskId, data, executorId = executorId, host = hostname)
            case TaskState.FAILED | TaskState.KILLED =>
              taskResultGetter.failTaskWithFailedReason(
                  taskSetMgr, mr3TaskId, state, data, executorId = executorId, host = hostname)
            case _ =>
              logWarning(s"Ignore TaskState in stateUpdate: $state, $mr3TaskId")
          }
        case None =>
          logError(
            ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
              "likely the result of receiving duplicate task finished status updates) or its " +
              "executor has been marked as failed.")
              .format(state, mr3TaskId))
      }
    } catch {
      case e: Exception => logError("Exception in statusUpdate", e)
    }
  }

  // returns immediately because dagScheduler.executorAdded() returns immediately
  def registerExecutor(executorId: String, hostname: String, data: ExecutorData): Unit = {
    dagScheduler.executorAdded(executorId, hostname)
    sc.listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
  }

  // returns immediately because dagScheduler.executorLost() returns immediately
  def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    dagScheduler.executorLost(executorId, reason)
    sc.listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
  }

  def removeExecutorFromBlockManagerMaster(executorId: String): Unit = {
    sc.env.blockManager.master.removeExecutorAsync(executorId)
  }

  def updateDelegationTokenManager(tokens: Array[Byte]): Unit = {
    val newCredentials = SparkHadoopUtil.get.deserialize(tokens)
    credentials.set(newCredentials)
  }

  //
  // registerTaskSetManagers/unregisterTaskSetManager()
  //

  // Invariant: inside taskSetManagersLock.synchronized{}
  private def registerTaskSetManagers(
      jobId: Int, dagClient: DAGClient, taskSetManagers: List[MR3TaskSetManager]): Unit = {
    assert { jobIdToTaskSetManagers.get(jobId).isEmpty && jobIdToDagStatusChecker.get(jobId).isEmpty }

    jobIdToTaskSetManagers.put(jobId, new mutable.HashSet[MR3TaskSetManager])
    taskSetManagers.foreach { taskSetManager =>
      jobIdToTaskSetManagers(jobId).add(taskSetManager)

      val stageIdAttempt = (taskSetManager.taskSet.stageId, taskSetManager.taskSet.stageAttemptId)
      assert { !taskSetManagersByStageIdAttempt.isDefinedAt(stageIdAttempt) }
      taskSetManagersByStageIdAttempt.put(stageIdAttempt, taskSetManager)

      taskSetManager.sparkTaskIdsForeach { taskId => taskSetManagersByTaskId.put(taskId, taskSetManager) }
      rootPool addSchedulable taskSetManager
    }

    val name = s"Job $jobId"
    val dagStatusChecker = new DAGStatusChecker(jobId, name, dagClient, dagStatusCheckerPeriodMs, backend = this)
    jobIdToDagStatusChecker.put(jobId, dagStatusChecker)
    dagStatusChecker.run()
  }

  // called from MR3TaskSetManager that makes the final state transition
  def unregisterTaskSetManager(taskSetManager: MR3TaskSetManager): Unit = {
    val numTaskSetManagers = taskSetManagersLock.synchronized {
      val jobId = taskSetManager.jobId
      val stageIdAttempt = (taskSetManager.taskSet.stageId, taskSetManager.taskSet.stageAttemptId)

      assert { taskSetManagersByStageIdAttempt.contains(stageIdAttempt) }
      taskSetManagersByStageIdAttempt.remove(stageIdAttempt)

      taskSetManager.sparkTaskIdsForeach { taskId => taskSetManagersByTaskId.remove(taskId) }
      rootPool removeSchedulable taskSetManager

      assert {
          jobIdToDagStatusChecker.isDefinedAt(jobId) &&
          jobIdToTaskSetManagers.isDefinedAt(jobId) &&
          jobIdToTaskSetManagers(jobId).contains(taskSetManager) }
      jobIdToTaskSetManagers(jobId).remove(taskSetManager)

      if (jobIdToTaskSetManagers(jobId).isEmpty) {
        // TODO: check if LOG_LEVEL == DEBUG?
        assert { taskSetManagersByStageIdAttempt.values.forall { _.jobId != jobId } }
        assert { taskSetManagersByTaskId.values.forall { _.jobId != jobId } }

        jobIdToTaskSetManagers.remove(jobId)
        val dagStatusChecker = jobIdToDagStatusChecker(jobId)
        dagStatusChecker.stop()
        jobIdToDagStatusChecker.remove(jobId)
      }

      taskSetManagersLock.notifyAll()
      taskSetManagersByStageIdAttempt.size
    }
    logInfo(s"Unregistered TaskSetManager: ${taskSetManager.taskSet}, ($numTaskSetManagers remaining)")
  }

  def unregisterJob(jobId: Int): Unit = taskSetManagersLock.synchronized {
    if (jobIdToTaskSetManagers.isDefinedAt(jobId)) {
      assert { jobIdToDagStatusChecker.isDefinedAt(jobId) }

      jobIdToTaskSetManagers(jobId).foreach { _.kill(s"Job $jobId failed") }

      // the below assertion may fail because TaskResultGetter may be unregistering the TaskSetManager
      // at the same time
      // assert { jobIdToDagStatusChecker.get(jobId).isEmpty && jobIdToTaskSetManagers.get(jobId).isEmpty }
    } else {
      logWarning(s"Attempt to stop Job $jobId, which is already finished.")
    }
  }
}
