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
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{RejectedExecutionException, ThreadPoolExecutor}

import mr3.api.client.{DAGClient, DAGState, DAGStatus, VertexState}
import javax.annotation.concurrent.GuardedBy
import org.apache.spark.{TaskFailedReason, UnknownReason}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Utils => SparkUtils}

/**
  * There are 3 available states of DAG: 1. Succeed, 2. Failed, 3. Killed. For 2. and 3., we report the
  * failure of the DAG to MR3Backend. For 1., however, we don't report the success of the DAG. This is
  * because handling TaskCompletionEvent may fail even if the DAG succeeded. Therefore, each Spark component
  * tracks the success of tasks and cleans up the state when it handles the last TaskCompletionEvent. For
  * this reason, we ignore the success of DAG and only focus on the failure of DAG here.
 */
class DAGStatusChecker(
    jobId: Int,
    name: String,
    dagClient: DAGClient,
    checkPeriodMs: Int,
    backend: DAGStatusCheckerInterface)
  extends Logging {

  @GuardedBy("this")
  private[this] val isStopped = new AtomicBoolean(false)

  private def dagStatusCheckerExecutor: ThreadPoolExecutor = backend.dagStatusCheckerExecutor

  // Invariant: Any thread that cleans up a Job from MR3Backend must call this method
  // before removing the DAGStatusChecker from MR3Backend.
  // Once this method is called, there should be no reference to this DAGStatusChecker.
  // use this.synchronized {} for the assertion.
  def stop(): Unit = this.synchronized {
    assert { !isStopped.get() }
    isStopped.set(true)
  }

  // This method should be called only by ShutdownHook. See MR3Backend.start() for more details.
  // To stop DAGStatusChecker and kill DAGClient, always use stop().
  def kill(): Unit = {
    dagClient.tryKillDag()
  }

  // do not call dagClient.close() because we use MR3SessionClient
  def run(): Unit = {
    try {
      dagStatusCheckerExecutor.execute(() => SparkUtils.logUncaughtExceptions {
        var shouldKillDag = isStopped.get
        var isFinished: Boolean = false
        var dagStatus: Option[DAGStatus] = None
        while (!isFinished && !shouldKillDag) {
          shouldKillDag = isStopped.get
          dagStatus = dagClient.getDagStatusWait(false, checkPeriodMs)
          isFinished = dagStatus.map { _.isFinished }.getOrElse(true)
        }
        logInfo(s"DAGStatusChecker stopped calling checkStatus(): $name, ${dagStatus.map(_.state)}, $shouldKillDag")

        if (!isFinished) {
          assert { shouldKillDag }  // because once set to true, shouldKillDag is never unset
          logInfo(s"MR3TaskSetManager in Killed or Stopped, so trying to kill DAG: $name")
          dagClient.tryKillDag()
        } else if (dagStatus.get.state != DAGState.Succeeded) {
          val failedStages = dagStatus.get.vertexStatusMap
            .filter { _._2.state == VertexState.Failed }
            .map { _._1.replace(BigDAG.WORKER_VERTEX_NAME_PREFIX, "").toInt }
          val taskFailedReason = getTaskFailedReason(dagStatus.get)
          backend.notifyJobFailed(jobId, failedStages.toSeq, taskFailedReason)
        }

        // keep calling DAGClient.getDagStatusWait() to call DAGClientHandler.acknowledgeDagFinished()
        while (dagStatus.nonEmpty && !dagStatus.get.isFinished) {
          dagStatus = dagClient.getDagStatusWait(false, checkPeriodMs)
        }
        logInfo(s"DAGStatusChecker thread finished: $name")
      })
    } catch {
      case _: RejectedExecutionException if backend.sparkEnv.isStopped =>
        // ignore it
    }
  }

  private def getTaskFailedReason(dagStatus: DAGStatus): TaskFailedReason = {
    val loader = SparkUtils.getContextOrSparkClassLoader
    try {
      val diagnostics = dagStatus.diagnostics   // either empty or a singleton list
      if (diagnostics.isEmpty) {
        logWarning(s"MR3TaskSetManager $name failed with no diagnostic message")
        UnknownReason
      } else {
        if (diagnostics.length > 1) {
          logWarning(s"DAGStatus contains more than one diagnostic message: ${diagnostics.length}")
        }
        val serializedTaskFailedReason = ByteBuffer.wrap(Base64.getDecoder.decode(diagnostics.head))
        val serializer = backend.sparkEnv.closureSerializer.newInstance()
        serializer.deserialize[TaskFailedReason](serializedTaskFailedReason, loader)
      }
    } catch {
      case _: ClassNotFoundException =>
        logError("Could not deserialize TaskFailedReason: ClassNotFound with classloader " + loader)
        UnknownReason
      case _: Throwable =>
        logError(s"Could not parse TaskFailedReason: ${dagStatus.diagnostics}")
        UnknownReason
    }
  }
}
