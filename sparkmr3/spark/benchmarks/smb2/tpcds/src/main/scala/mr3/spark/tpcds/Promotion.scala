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

case object Promotion extends TPCDS {
  val schema = StructType(
    StructField("p_promo_sk", LongType) ::
      StructField("p_promo_id", StringType) ::
      StructField("p_start_date_sk", LongType) ::
      StructField("p_end_date_sk", LongType) ::
      StructField("p_item_sk", LongType) ::
      StructField("p_cost", DoubleType) ::
      StructField("p_response_target", IntegerType) ::
      StructField("p_promo_name", StringType) ::
      StructField("p_channel_dmail", StringType) ::
      StructField("p_channel_email", StringType) ::
      StructField("p_channel_catalog", StringType) ::
      StructField("p_channel_tv", StringType) ::
      StructField("p_channel_radio", StringType) ::
      StructField("p_channel_press", StringType) ::
      StructField("p_channel_event", StringType) ::
      StructField("p_channel_demo", StringType) ::
      StructField("p_channel_details", StringType) ::
      StructField("p_purpose", StringType) ::
      StructField("p_discount_active", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      long(words(2)),
      long(words(3)),
      long(words(4)),
      double(words(5)),
      int(words(6)),
      string(words(7)),
      string(words(8)),
      string(words(9)),
      string(words(10)),
      string(words(11)),
      string(words(12)),
      string(words(13)),
      string(words(14)),
      string(words(15)),
      string(words(16)),
      string(words(17)),
      string(words(18))
    ))
  }
}
