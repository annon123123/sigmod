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

case object Customer extends TPCDS {
  val schema = StructType(
    StructField("c_customer_sk", LongType) ::
      StructField("c_customer_id", StringType) ::
      StructField("c_current_cdemo_sk", LongType) ::
      StructField("c_current_hdemo_sk", LongType) ::
      StructField("c_current_addr_sk", LongType) ::
      StructField("c_first_shipto_date_sk", LongType) ::
      StructField("c_first_sales_date_sk", LongType) ::
      StructField("c_salutation", StringType) ::
      StructField("c_first_name", StringType) ::
      StructField("c_last_name", StringType) ::
      StructField("c_preferred_cust_flag", StringType) ::
      StructField("c_birth_day", IntegerType) ::
      StructField("c_birth_month", IntegerType) ::
      StructField("c_birth_year", IntegerType) ::
      StructField("c_birth_country", StringType) ::
      StructField("c_login", StringType) ::
      StructField("c_email_address", StringType) ::
      StructField("c_last_review_date", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      long(words(2)),
      long(words(3)),
      long(words(4)),
      long(words(5)),
      long(words(6)),
      string(words(7)),
      string(words(8)),
      string(words(9)),
      string(words(10)),
      int(words(11)),
      int(words(12)),
      int(words(13)),
      string(words(14)),
      string(words(15)),
      string(words(16)),
      string(words(17))
    ))
  }
}
