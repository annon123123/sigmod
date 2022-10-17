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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case object TimeDim extends TPCDS {
  val schema = StructType(
    StructField("t_time_sk", LongType) ::
      StructField("t_time_id", StringType) ::
      StructField("t_time", IntegerType) ::
      StructField("t_hour", IntegerType) ::
      StructField("t_minute", IntegerType) ::
      StructField("t_second", IntegerType) ::
      StructField("t_am_pm", StringType) ::
      StructField("t_shift", StringType) ::
      StructField("t_sub_shift", StringType) ::
      StructField("t_meal_time", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      int(words(2)),
      int(words(3)),
      int(words(4)),
      int(words(5)),
      string(words(6)),
      string(words(7)),
      string(words(8)),
      string(words(9))
    ))
  }
}
