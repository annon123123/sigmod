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

case object Store extends TPCDS {
  val schema = StructType(
    StructField("s_store_sk", LongType) ::
      StructField("s_store_id", StringType) ::
      StructField("s_rec_start_date", StringType) ::
      StructField("s_rec_end_date", StringType) ::
      StructField("s_closed_date_sk", LongType) ::
      StructField("s_store_name", StringType) ::
      StructField("s_number_employees", IntegerType) ::
      StructField("s_floor_space", IntegerType) ::
      StructField("s_hours", StringType) ::
      StructField("s_manager", StringType) ::
      StructField("s_market_id", IntegerType) ::
      StructField("s_geography_class", StringType) ::
      StructField("s_market_desc", StringType) ::
      StructField("s_market_manager", StringType) ::
      StructField("s_division_id", IntegerType) ::
      StructField("s_division_name", StringType) ::
      StructField("s_company_id", IntegerType) ::
      StructField("s_company_name", StringType) ::
      StructField("s_street_number", StringType) ::
      StructField("s_street_name", StringType) ::
      StructField("s_street_type", StringType) ::
      StructField("s_suite_number", StringType) ::
      StructField("s_city", StringType) ::
      StructField("s_county", StringType) ::
      StructField("s_state", StringType) ::
      StructField("s_zip", StringType) ::
      StructField("s_country", StringType) ::
      StructField("s_gmt_offset", DoubleType) ::
      StructField("s_tax_precentage", DoubleType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      string(words(3)),
      long(words(4)),
      string(words(5)),
      int(words(6)),
      int(words(7)),
      string(words(8)),
      string(words(9)),
      int(words(10)),
      string(words(11)),
      string(words(12)),
      string(words(13)),
      int(words(14)),
      string(words(15)),
      int(words(16)),
      string(words(17)),
      string(words(18)),
      string(words(19)),
      string(words(20)),
      string(words(21)),
      string(words(22)),
      string(words(23)),
      string(words(24)),
      string(words(25)),
      string(words(26)),
      double(words(27)),
      double(words(28))
    ))
  }
}
