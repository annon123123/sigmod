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
import java.util.concurrent.atomic.AtomicInteger

import mr3.DAGAPI
import mr3.DAGAPI.{ContainerGroupProto, DAGProto, DaemonVertexProto, EdgeDataMovementTypeProto, EdgeProto, EntityDescriptorProto, SparkPartitionInfoProto, SparkStageInfoProto, SparkWorkerVertexManagerPayloadProto, UserPayloadProto, VertexProto}
import com.google.protobuf.ByteString
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.Credentials
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.mr3.SparkMR3Config.{SPARK_MR3_PIPELINED_SCHEDULE_SRC_FRACTION, SPARK_MR3_PIPELINED_SCHEDULE_SRC_FRACTION_DEFAULT}

import scala.collection.{Map, mutable}

case class SparkStageMetaData(
    shuffleId: Option[Int],
    numTasks: Int,
    numPartitions: Int, // For ShuffleMapStage, numPartitions = numTasks + preparedPartitions.length
    parentStageIds: Seq[Int]
)

// TODO: rename this file to DAG
private[mr3] object BigDAG extends Logging {
  private[this] val nextDagId = new AtomicInteger(0)

  private val CONTAINER_GROUP_PRIORITY = 0

  private[mr3] val WORKER_VERTEX_NAME_PREFIX = "SparkWorkerVertex-"
  private val WORKER_VERTEX_PROCESSOR_PREFIX = "spark.worker."

  private val CONTAINER_GROUP_NAME_PREFIX = "SparkContainerGroup-"

  private val DAEMON_VERTEX_NAME = "SparkDaemonVertex"
  private val DAEMON_VERTEX_PROCESSOR_NAME = "spark.daemon"

  def createDag(
      applicationId: String,
      jobId: Int,
      sparkStageMetaDataMap: Map[Int, SparkStageMetaData],
      sparkStageInfoProtoMap: Map[Int, SparkStageInfoProto.Builder],
      partitionAndTaskLocationMap: Map[Int, Seq[(DAGAPI.SparkPartitionInfoProto.Builder, Seq[String])]],
      driverUrl: String,
      sparkConf: SparkConf,
      dagConf: DAGAPI.ConfigurationProto.Builder,
      credentials: Option[Credentials],
      ioEncryptionKey: Option[Array[Byte]]): DAGProto = {
    val containerGroup = getContainerGroupBuilder(
        applicationId = applicationId, driverUrl = driverUrl, sparkConf, ioEncryptionKey)

    val dagProtoBuilder = DAGProto.newBuilder
      .setName(s"${applicationId}_${jobId}_${nextDagId.getAndIncrement()}")
      .addContainerGroups(containerGroup)
      .setDagConf(dagConf)
      .setJobPriority(jobId)

    val workerVertexManagerPayloadMap = createWorkerVertexManagerPayload(sparkStageMetaDataMap, sparkConf)

    val vertexProtoMap = new mutable.HashMap[Int, VertexProto.Builder]
    sparkStageMetaDataMap.foreach { case (stageId, stageMetaData) =>
      val parents = stageMetaData.parentStageIds

      // to check the priority of vertex
      assert { parents.forall { _ < stageId } }
      val vertex =
        getWorkerVertexBuilder(
          applicationId,
          stageId = stageId,
          sparkConf,
          numTasks = stageMetaData.numTasks,
          workerVertexManagerPayloadMap(stageId),
          sparkStageInfoProtoMap(stageId),
          partitionAndTaskLocationMap(stageId))
      vertexProtoMap.put(stageId, vertex)
    }

    val sourceDestStageIdPairList = sparkStageMetaDataMap.toSeq.flatMap { case (stageId, stageMetaData) =>
      stageMetaData.parentStageIds.map { (_, stageId) }
    }
    sourceDestStageIdPairList.foreach { case (srcStageId, dstStageId) =>
      val srcStageMetaData = sparkStageMetaDataMap(srcStageId)
      val edge =
        getEdgeBuilder(
          srcStageId = srcStageId,
          dstStageId = dstStageId,
          srcStageShuffleId = srcStageMetaData.shuffleId.get,
          srcStageNumPartitions = srcStageMetaData.numPartitions)
      vertexProtoMap(srcStageId).addOutEdgeIds(edge.getId)
      vertexProtoMap(dstStageId).addInEdgeIds(edge.getId)
      dagProtoBuilder.addEdges(edge)
    }

    vertexProtoMap.values.foreach { dagProtoBuilder.addVertices }

    credentials.foreach { creds =>
      val dob = new DataOutputBuffer
      creds.writeTokenStorageToStream(dob)
      val buffer = ByteBuffer.wrap(dob.getData, 0, dob.getLength)
      dagProtoBuilder.setCredentials(ByteString.copyFrom(buffer))
    }
    dagProtoBuilder.build
  }

  def getContainerGroupBuilder(
      applicationId: String,
      driverUrl: String,
      sparkConf: SparkConf,
      ioEncryptionKey: Option[Array[Byte]]): ContainerGroupProto.Builder = {
    val daemonVertex = getDaemonVertexBuilder(driverUrl, sparkConf, ioEncryptionKey)

    // use sparkConf because containerGroupResource is fixed for all DAGs
    val nonNativeMemoryMb = Utils.getContainerGroupNonNativeMemoryMb(sparkConf)
    val nativeMemoryMb = Utils.getOffHeapMemoryMb(sparkConf)
    val containerGroupResource = getResource(
      cores = Utils.getContainerGroupCores(sparkConf),
      memoryMb = nonNativeMemoryMb + nativeMemoryMb)
    val containerConfig = DAGAPI.ContainerConfigurationProto.newBuilder
      .setResource(containerGroupResource)
      .setNativeMemoryMb(nativeMemoryMb)

    ContainerGroupProto.newBuilder
      .setName(getContainerGroupName(applicationId))
      .setPriority(CONTAINER_GROUP_PRIORITY)
      .setContainerConfig(containerConfig)
      .addDaemonVertices(daemonVertex)
  }

  private def getDaemonVertexBuilder(
      driverUrl: String,
      sparkConf: SparkConf,
      ioEncryptionKey: Option[Array[Byte]]): DaemonVertexProto.Builder = {
    val sparkConfProto = DAGAPI.ConfigurationProto.newBuilder
    sparkConf.getAll.foreach { case (key, value) =>
      val keyValueProto = DAGAPI.KeyValueProto.newBuilder
        .setKey(key)
        .setValue(value)
      sparkConfProto.addConfKeyValues(keyValueProto)
    }
    ioEncryptionKey.foreach { ioEncryptionKey =>
      val keyValueProto = DAGAPI.KeyValueProto.newBuilder
        .setKey("spark.mr3.io.encryption.key")
        .setValueBytes(ByteString.copyFrom(ioEncryptionKey))
      sparkConfProto.addConfKeyValues(keyValueProto)
    }

    val daemonConfigProto = DAGAPI.DaemonConfigProto.newBuilder
      .setAddress(driverUrl)
      .setConfig(sparkConfProto)

    val daemonConfigByteString = daemonConfigProto.build.toByteString
    val daemonVertexManagerPayload = DAGAPI.UserPayloadProto.newBuilder
      .setPayload(daemonConfigByteString)

    val daemonVertexManagerPlugin = EntityDescriptorProto.newBuilder
      .setClassName(DAEMON_VERTEX_NAME)
      .setUserPayload(daemonVertexManagerPayload)

    val daemonVertexProcessor = EntityDescriptorProto.newBuilder
      .setClassName(DAEMON_VERTEX_PROCESSOR_NAME)
    val daemonVertexResource = getResource(cores = 0, memoryMb = 0)

    DaemonVertexProto.newBuilder
      .setName(DAEMON_VERTEX_NAME)
      .setProcessor(daemonVertexProcessor)
      .setResource(daemonVertexResource)
      .setVertexManagerPlugin(daemonVertexManagerPlugin)
  }

  private def getWorkerVertexBuilder(
      applicationId: String,
      stageId: Int,
      sparkConf: SparkConf,
      numTasks: Int,
      workerVertexManagerPayloadProto: SparkWorkerVertexManagerPayloadProto.Builder,
      sparkStageInfoProto: SparkStageInfoProto.Builder,
      partitionAndTaskLocation: Seq[(SparkPartitionInfoProto.Builder, Seq[String])]): VertexProto.Builder = {
    val sparkStageInfoByteString = sparkStageInfoProto.build.toByteString
    val workerProcessorPayload = DAGAPI.UserPayloadProto.newBuilder
      .setPayload(sparkStageInfoByteString)

    val workerVertexProcessor = EntityDescriptorProto.newBuilder
      .setClassName(WORKER_VERTEX_PROCESSOR_PREFIX + stageId)
      .setUserPayload(workerProcessorPayload)

    val workerVertexResource = getResource(
        cores = Utils.getTaskCores(sparkConf),
        memoryMb = Utils.getTaskMemoryMb(sparkConf))

    val vertexProtoBuilder = VertexProto.newBuilder
      .setName(getVertexName(stageId))
      .setContainerGroupName(getContainerGroupName(applicationId))
      .setNumTasks(numTasks)
      .setProcessor(workerVertexProcessor)
      .setResource(workerVertexResource)
      .setPriority(stageId)

    partitionAndTaskLocation.foreach { case (partitionInfoProto, taskLocationHint) =>
      val taskLocationHintProto = DAGAPI.TaskLocationHintProto.newBuilder
      taskLocationHint.foreach { taskLocationHintProto.addHosts }
      taskLocationHintProto.setAnyHost(true)

      vertexProtoBuilder.addTaskLocationHints(taskLocationHintProto)
      workerVertexManagerPayloadProto.addPartition(partitionInfoProto)
    }

    val workerVertexManagerPayloadByteString = workerVertexManagerPayloadProto.build.toByteString
    val workerVertexManagerPayload = DAGAPI.UserPayloadProto.newBuilder
      .setPayload(workerVertexManagerPayloadByteString)

    val workerVertexManager = EntityDescriptorProto.newBuilder
      .setClassName(WORKER_VERTEX_NAME_PREFIX + stageId)
      .setUserPayload(workerVertexManagerPayload)

    vertexProtoBuilder.setVertexManagerPlugin(workerVertexManager)

    vertexProtoBuilder
  }

  private def getEdgeBuilder(
      srcStageId: Int,
      dstStageId: Int,
      srcStageShuffleId: Int,
      srcStageNumPartitions: Int): EdgeProto.Builder = {
    val srcLogicalOutput = EntityDescriptorProto.newBuilder
        .setClassName(s"SrcLogicalOutput-$srcStageId")

    val destLogicalInput = {
      val destLogicalInputPayloadProto = DAGAPI.SparkLogicalInputPayloadProto.newBuilder
        .setShuffleId(srcStageShuffleId)
        .setShuffleNumPartitions(srcStageNumPartitions)

      val dstLogicalInputUserPayload = UserPayloadProto.newBuilder
        .setPayload(destLogicalInputPayloadProto.build.toByteString)
      EntityDescriptorProto.newBuilder
        .setClassName(s"DestLogicalInput-$dstStageId")
        .setUserPayload(dstLogicalInputUserPayload)
    }

    EdgeProto.newBuilder
      .setId(s"SparkEdge_$srcStageId->$dstStageId")
      .setInputVertexName(getVertexName(srcStageId))
      .setOutputVertexName(getVertexName(dstStageId))
      .setDataMovementType(EdgeDataMovementTypeProto.BROADCAST)
      .setSrcLogicalOutput(srcLogicalOutput)
      .setDestLogicalInput(destLogicalInput)
  }

  private def getContainerGroupName(applicationId: String): String =
    CONTAINER_GROUP_NAME_PREFIX + applicationId

  private def getVertexName(stageId: Int): String = WORKER_VERTEX_NAME_PREFIX + stageId

  private def getResource(cores: Int, memoryMb: Int): DAGAPI.ResourceProto.Builder = {
    DAGAPI.ResourceProto.newBuilder
      .setVirtualCores(cores)
      .setMemoryMb(memoryMb)
      .setCoreDivisor(1)
  }

  private def createWorkerVertexManagerPayload(
      sparkStageMetaDataMap: Map[Int, SparkStageMetaData],
      sparkConf: SparkConf): Map[Int, SparkWorkerVertexManagerPayloadProto.Builder] = {
    val srcTasksFraction = sparkConf.getDouble(
      SPARK_MR3_PIPELINED_SCHEDULE_SRC_FRACTION,
      SPARK_MR3_PIPELINED_SCHEDULE_SRC_FRACTION_DEFAULT)
    require({ 0D <= srcTasksFraction && srcTasksFraction <= 1D },
      s"$SPARK_MR3_PIPELINED_SCHEDULE_SRC_FRACTION must be in [0, 1].")

    val numTasksMap = sparkStageMetaDataMap.mapValues { _.numTasks }
    val payloadMap = new mutable.HashMap[Int, SparkWorkerVertexManagerPayloadProto.Builder]
    sparkStageMetaDataMap.foreach { case (stageId, stageMetaData) =>
      val parents = stageMetaData.parentStageIds
      val vertexManagerPayload = SparkWorkerVertexManagerPayloadProto.newBuilder

      val numFinishedSrcTasksThreshold =
        if (srcTasksFraction == 0D || parents.isEmpty) {
          0L
        } else {
          val numSrcTasks = parents.map { p => numTasksMap(p).toLong }.sum
          if (srcTasksFraction == 1D) {
            numSrcTasks
          } else {
            math.floor(numSrcTasks * srcTasksFraction).toLong
          }
        }
      vertexManagerPayload.setNumFinishedSrcTasksThreshold(numFinishedSrcTasksThreshold)

      parents.foreach { parentStageId =>
        val vertexNameNumTasksProto = DAGAPI.SparkVertexNameNumTasksProto.newBuilder
          .setVertexName(getVertexName(parentStageId))
          .setNumTasks(numTasksMap(parentStageId))
        vertexManagerPayload.addSrcVertexNameAndNumTasksList(vertexNameNumTasksProto)
      }
      payloadMap(stageId) = vertexManagerPayload
    }

    payloadMap
  }
}
