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

case object Warehouse extends TPCDS {
  val schema = StructType(
    StructField("w_warehouse_sk", LongType) ::
      StructField("w_warehouse_id", StringType) ::
      StructField("w_warehouse_name", StringType) ::
      StructField("w_warehouse_sq_ft", IntegerType) ::
      StructField("w_street_number", StringType) ::
      StructField("w_street_name", StringType) ::
      StructField("w_street_type", StringType) ::
      StructField("w_suite_number", StringType) ::
      StructField("w_city", StringType) ::
      StructField("w_county", StringType) ::
      StructField("w_state", StringType) ::
      StructField("w_zip", StringType) ::
      StructField("w_country", StringType) ::
      StructField("w_gmt_offset", DoubleType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      int(words(3)),
      string(words(4)),
      string(words(5)),
      string(words(6)),
      string(words(7)),
      string(words(8)),
      string(words(9)),
      string(words(10)),
      string(words(11)),
      string(words(12)),
      double(words(13))
    ))
  }
}
