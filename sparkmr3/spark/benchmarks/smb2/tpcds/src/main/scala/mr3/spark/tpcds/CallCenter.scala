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

case object CallCenter extends TPCDS {
  val schema = StructType(
    StructField("cc_call_center_sk", LongType) ::
      StructField("cc_call_center_id", StringType) ::
      StructField("cc_rec_start_date", StringType) ::
      StructField("cc_rec_end_date", StringType) ::
      StructField("cc_closed_date_sk", LongType) ::
      StructField("cc_open_date_sk", LongType) ::
      StructField("cc_name", StringType) ::
      StructField("cc_class", StringType) ::
      StructField("cc_employees", IntegerType) ::
      StructField("cc_sq_ft", IntegerType) ::
      StructField("cc_hours", StringType) ::
      StructField("cc_manager", StringType) ::
      StructField("cc_mkt_id", IntegerType) ::
      StructField("cc_mkt_class", StringType) ::
      StructField("cc_mkt_desc", StringType) ::
      StructField("cc_market_manager", StringType) ::
      StructField("cc_division", IntegerType) ::
      StructField("cc_division_name", StringType) ::
      StructField("cc_company", IntegerType) ::
      StructField("cc_company_name", StringType) ::
      StructField("cc_street_number", StringType) ::
      StructField("cc_street_name", StringType) ::
      StructField("cc_street_type", StringType) ::
      StructField("cc_suite_number", StringType) ::
      StructField("cc_city", StringType) ::
      StructField("cc_county", StringType) ::
      StructField("cc_state", StringType) ::
      StructField("cc_zip", StringType) ::
      StructField("cc_country", StringType) ::
      StructField("cc_gmt_offset", DoubleType) ::
      StructField("cc_tax_percentage", DoubleType) :: Nil
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
      string(words(7)),
      int(words(8)),
      int(words(9)),
      string(words(10)),
      string(words(11)),
      int(words(12)),
      string(words(13)),
      string(words(14)),
      string(words(15)),
      int(words(16)),
      string(words(17)),
      int(words(18)),
      string(words(19)),
      string(words(20)),
      string(words(21)),
      string(words(22)),
      string(words(23)),
      string(words(24)),
      string(words(25)),
      string(words(26)),
      string(words(27)),
      string(words(28)),
      double(words(29)),
      double(words(30))
    ))
  }
}
