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

package mr3.spark.tpcds

import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object RunManyQueries {
  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println("Usage: RunManyQueries <TPC-DS db path> <Query root path> <Query num seq> <# of threads> <# of iterations> <mix sequence>")
      println("Example:")
      println("  RunManyQueries \"hdfs:///tpcds-data\" \"/query-sample\" 2,4,6,8 4 2 false")
    } else {
      val tpcdsPath = args(0)
      val queryDir = args(1)
      val querySeq = args(2).split(",").toList
      val numThread = args(3).toInt
      val numIter = args(4).toInt
      val enableMix = args(5).toBoolean

      val sparkSession = setupSparkSession(tpcdsPath)
      val queries = getQueries(queryDir, querySeq, sparkSession.sparkContext.hadoopConfiguration)

      val executorService = Executors.newFixedThreadPool(numThread)
      val executionContext = ExecutionContext.fromExecutor(executorService)
      val latch = new CountDownLatch(numThread)

      (0 until numThread).map { threadId =>
        Future {
          try {
            runQueries(threadId, sparkSession, queries, numIter, enableMix)
          } finally {
            latch.countDown()
          }
        } (executionContext)
      }

      try {
        latch.await()
      } finally {
        executorService.shutdownNow()
        sparkSession.stop()
      }
    }
  }

  private def setupSparkSession(tpcdsPath: String): SparkSession = {
    val sc = SparkContext.getOrCreate()
    val spark = new SparkSession.Builder().getOrCreate()

    TPCDS.tables foreach {
      case (name, table) =>
        val rdd1 = sc.textFile(tpcdsPath + "/" + name + "/")
        val rdd2 = TPCDS.splitRDD(rdd1)
        val rdd3 = table.parseRDD(rdd2)

        val df = spark.createDataFrame(rdd3, table.schema)
        df.createOrReplaceTempView(name)
        println(s"Read table: $name")
    }

    spark
  }

  private def getQueries(queryDir: String, queryNames: List[String], conf: Configuration): List[(String, String)] = {
    queryNames map { queryName => (queryName, getQuery(queryDir, queryName, conf)) }
  }

  private def getQuery(queryDir: String, queryName: String, conf: Configuration): String = {
    val queryPath = s"$queryDir/query$queryName.sql"
    val path = new Path(queryPath)
    val fs = path.getFileSystem(conf)
    val stream = fs.open(path)
    IOUtils.toString(stream)
  }

  private def runQueries(
      threadId: Int,
      sparkSession: SparkSession,
      queries: List[(String, String)],
      numIter: Int,
      enableMix: Boolean): Unit = {
    for (iteration <- 0 until numIter) {
      val querySeq = if (enableMix) Random.shuffle(queries) else queries
      var count = 0
      for ((queryName, query) <- querySeq) {
        val start = System.currentTimeMillis()
        try {
          val numRows = sparkSession.sql(query).count()
          println(s"Thread: $threadId, Iter: $iteration-$count, Query: $queryName, Time: ${getElapsedTime(start)}, Selected rows: $numRows")
        } catch {
          case t: Throwable =>
            println(s"Thread: $threadId, Iter: $iteration-$count, Query: $queryName, Time: ${getElapsedTime(start)}, Failed")
            println(t)
        }
        count = count + 1
      }
    }
  }

  private def getElapsedTime(start: Long): Long = {
    System.currentTimeMillis() - start
  }
}

