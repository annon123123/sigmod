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

case object StoreSales extends TPCDS {
  val schema = StructType(
    StructField("ss_sold_date_sk", LongType) ::
      StructField("ss_sold_time_sk", LongType) ::
      StructField("ss_item_sk", LongType) ::
      StructField("ss_customer_sk", LongType) ::
      StructField("ss_cdemo_sk", LongType) ::
      StructField("ss_hdemo_sk", LongType) ::
      StructField("ss_addr_sk", LongType) ::
      StructField("ss_store_sk", LongType) ::
      StructField("ss_promo_sk", LongType) ::
      StructField("ss_ticket_number", LongType) ::
      StructField("ss_quantity", IntegerType) ::
      StructField("ss_wholesale_cost", DoubleType) ::
      StructField("ss_list_price", DoubleType) ::
      StructField("ss_sales_price", DoubleType) ::
      StructField("ss_ext_discount_amt", DoubleType) ::
      StructField("ss_ext_sales_price", DoubleType) ::
      StructField("ss_ext_wholesale_cost", DoubleType) ::
      StructField("ss_ext_list_price", DoubleType) ::
      StructField("ss_ext_tax", DoubleType) ::
      StructField("ss_coupon_amt", DoubleType) ::
      StructField("ss_net_paid", DoubleType) ::
      StructField("ss_net_paid_inc_tax", DoubleType) ::
      StructField("ss_net_profit", DoubleType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      long(words(1)),
      long(words(2)),
      long(words(3)),
      long(words(4)),
      long(words(5)),
      long(words(6)),
      long(words(7)),
      long(words(8)),
      long(words(9)),
      int(words(10)),
      double(words(11)),
      double(words(12)),
      double(words(13)),
      double(words(14)),
      double(words(15)),
      double(words(16)),
      double(words(17)),
      double(words(18)),
      double(words(19)),
      double(words(20)),
      double(words(21)),
      double(words(22))
    ))
  }
}
