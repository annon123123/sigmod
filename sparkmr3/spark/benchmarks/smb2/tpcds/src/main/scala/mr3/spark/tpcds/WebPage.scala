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

case object WebPage extends TPCDS {
  val schema = StructType(
    StructField("wp_web_page_sk", LongType) ::
      StructField("wp_web_page_id", StringType) ::
      StructField("wp_rec_start_date", StringType) ::
      StructField("wp_rec_end_date", StringType) ::
      StructField("wp_creation_date_sk", LongType) ::
      StructField("wp_access_date_sk", LongType) ::
      StructField("wp_autogen_flag", StringType) ::
      StructField("wp_customer_sk", LongType) ::
      StructField("wp_url", StringType) ::
      StructField("wp_type", StringType) ::
      StructField("wp_char_count", IntegerType) ::
      StructField("wp_link_count", IntegerType) ::
      StructField("wp_image_count", IntegerType) ::
      StructField("wp_max_ad_count", IntegerType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      string(words(3)),
      long(words(4)),
      long(words(5)),
      string(words(6)),
      long(words(7)),
      string(words(8)),
      string(words(9)),
      int(words(10)),
      int(words(11)),
      int(words(12)),
      int(words(13))
    ))
  }
}
