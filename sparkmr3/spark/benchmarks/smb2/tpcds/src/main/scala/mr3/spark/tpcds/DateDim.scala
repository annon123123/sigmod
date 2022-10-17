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

case object DateDim extends TPCDS {
  val schema = StructType(
    StructField("d_date_sk", LongType) ::
      StructField("d_date_id", StringType) ::
      StructField("d_date", StringType) ::
      StructField("d_month_seq", IntegerType) ::
      StructField("d_week_seq", IntegerType) ::
      StructField("d_quarter_seq", IntegerType) ::
      StructField("d_year", IntegerType) ::
      StructField("d_dow", IntegerType) ::
      StructField("d_moy", IntegerType) ::
      StructField("d_dom", IntegerType) ::
      StructField("d_qoy", IntegerType) ::
      StructField("d_fy_year", IntegerType) ::
      StructField("d_fy_quarter_seq", IntegerType) ::
      StructField("d_fy_week_seq", IntegerType) ::
      StructField("d_day_name", StringType) ::
      StructField("d_quarter_name", StringType) ::
      StructField("d_holiday", StringType) ::
      StructField("d_weekend", StringType) ::
      StructField("d_following_holiday", StringType) ::
      StructField("d_first_dom", IntegerType) ::
      StructField("d_last_dom", IntegerType) ::
      StructField("d_same_day_ly", IntegerType) ::
      StructField("d_same_day_lq", IntegerType) ::
      StructField("d_current_day", StringType) ::
      StructField("d_current_week", StringType) ::
      StructField("d_current_month", StringType) ::
      StructField("d_current_quarter", StringType) ::
      StructField("d_current_year", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      int(words(3)),
      int(words(4)),
      int(words(5)),
      int(words(6)),
      int(words(7)),
      int(words(8)),
      int(words(9)),
      int(words(10)),
      int(words(11)),
      int(words(12)),
      int(words(13)),
      string(words(14)),
      string(words(15)),
      string(words(16)),
      string(words(17)),
      string(words(18)),
      int(words(19)),
      int(words(20)),
      int(words(21)),
      int(words(22)),
      string(words(23)),
      string(words(24)),
      string(words(25)),
      string(words(26)),
      string(words(27))
    ))
  }
}
