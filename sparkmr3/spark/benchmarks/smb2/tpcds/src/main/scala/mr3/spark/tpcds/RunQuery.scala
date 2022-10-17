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

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object RunQuery {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(s"Usage: RunQuery <TPC-DS db path> <Query root path> <Query num>")
    } else {
      val tpcdsPath = args(0)
      val queryDir = args(1)
      val queryNum = args(2).toInt

      val result = runQuery(tpcdsPath, queryDir, queryNum)

      val count = result.count()
      println(s"Selected Rows: $count")
      println(s"Result of Query $queryNum: ")
      result.show(20, false)
    }
  }

  private def runQuery(tpcdsPath: String, queryDir: String, queryNum: Int): DataFrame = {
    val sc = SparkContext.getOrCreate()
    val spark = new SparkSession.Builder().getOrCreate()

    setupUsingSparkContext(sc, spark, tpcdsPath)

    val queryPath = s"$queryDir/query$queryNum.sql"
    val query = Source.fromFile(queryPath).mkString

    spark.sql(query)
  }

  private def setupUsingSparkContext(sc: SparkContext, spark: SparkSession, tpcdsPath: String): Unit = {
    TPCDS.tables foreach {
      case (name, table) =>
        val rdd1 = sc.textFile(tpcdsPath + "/" + name + "/")
        val rdd2 = TPCDS.splitRDD(rdd1)
        val rdd3 = table.parseRDD(rdd2)

        val df = spark.createDataFrame(rdd3, table.schema)
        df.createOrReplaceTempView(name)
        println(s"Read table: $name")
    }
  }
}
