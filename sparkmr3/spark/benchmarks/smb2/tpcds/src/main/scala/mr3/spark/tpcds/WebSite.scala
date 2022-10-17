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

case object WebSite extends TPCDS {
  val schema = StructType(
    StructField("web_site_sk", LongType) ::
      StructField("web_site_id", StringType) ::
      StructField("web_rec_start_date", StringType) ::
      StructField("web_rec_end_date", StringType) ::
      StructField("web_name", StringType) ::
      StructField("web_open_date_sk", LongType) ::
      StructField("web_close_date_sk", LongType) ::
      StructField("web_class", StringType) ::
      StructField("web_manager", StringType) ::
      StructField("web_mkt_id", IntegerType) ::
      StructField("web_mkt_class", StringType) ::
      StructField("web_mkt_desc", StringType) ::
      StructField("web_market_manager", StringType) ::
      StructField("web_company_id", IntegerType) ::
      StructField("web_company_name", StringType) ::
      StructField("web_street_number", StringType) ::
      StructField("web_street_name", StringType) ::
      StructField("web_street_type", StringType) ::
      StructField("web_suite_number", StringType) ::
      StructField("web_city", StringType) ::
      StructField("web_county", StringType) ::
      StructField("web_state", StringType) ::
      StructField("web_zip", StringType) ::
      StructField("web_country", StringType) ::
      StructField("web_gmt_offset", DoubleType) ::
      StructField("web_tax_percentage", DoubleType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      string(words(3)),
      string(words(4)),
      long(words(5)),
      long(words(6)),
      string(words(7)),
      string(words(8)),
      int(words(9)),
      string(words(10)),
      string(words(11)),
      string(words(12)),
      int(words(13)),
      string(words(14)),
      string(words(15)),
      string(words(16)),
      string(words(17)),
      string(words(18)),
      string(words(19)),
      string(words(20)),
      string(words(21)),
      string(words(22)),
      string(words(23)),
      double(words(24)),
      double(words(25))
    ))
  }
}
