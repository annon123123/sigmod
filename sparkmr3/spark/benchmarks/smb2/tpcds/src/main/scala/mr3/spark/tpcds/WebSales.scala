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

case object WebSales extends TPCDS {
  val schema = StructType(
    StructField("ws_sold_date_sk", LongType) ::
      StructField("ws_sold_time_sk", LongType) ::
      StructField("ws_ship_date_sk", LongType) ::
      StructField("ws_item_sk", LongType) ::
      StructField("ws_bill_customer_sk", LongType) ::
      StructField("ws_bill_cdemo_sk", LongType) ::
      StructField("ws_bill_hdemo_sk", LongType) ::
      StructField("ws_bill_addr_sk", LongType) ::
      StructField("ws_ship_customer_sk", LongType) ::
      StructField("ws_ship_cdemo_sk", LongType) ::
      StructField("ws_ship_hdemo_sk", LongType) ::
      StructField("ws_ship_addr_sk", LongType) ::
      StructField("ws_web_page_sk", LongType) ::
      StructField("ws_web_site_sk", LongType) ::
      StructField("ws_ship_mode_sk", LongType) ::
      StructField("ws_warehouse_sk", LongType) ::
      StructField("ws_promo_sk", LongType) ::
      StructField("ws_order_number", LongType) ::
      StructField("ws_quantity", IntegerType) ::
      StructField("ws_wholesale_cost", DoubleType) ::
      StructField("ws_list_price", DoubleType) ::
      StructField("ws_sales_price", DoubleType) ::
      StructField("ws_ext_discount_amt", DoubleType) ::
      StructField("ws_ext_sales_price", DoubleType) ::
      StructField("ws_ext_wholesale_cost", DoubleType) ::
      StructField("ws_ext_list_price", DoubleType) ::
      StructField("ws_ext_tax", DoubleType) ::
      StructField("ws_coupon_amt", DoubleType) ::
      StructField("ws_ext_ship_cost", DoubleType) ::
      StructField("ws_net_paid", DoubleType) ::
      StructField("ws_net_paid_inc_tax", DoubleType) ::
      StructField("ws_net_paid_inc_ship", DoubleType) ::
      StructField("ws_net_paid_inc_ship_tax", DoubleType) ::
      StructField("ws_net_profit", DoubleType) :: Nil
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
      long(words(10)),
      long(words(11)),
      long(words(12)),
      long(words(13)),
      long(words(14)),
      long(words(15)),
      long(words(16)),
      long(words(17)),
      int(words(18)),
      double(words(19)),
      double(words(20)),
      double(words(21)),
      double(words(22)),
      double(words(23)),
      double(words(24)),
      double(words(25)),
      double(words(26)),
      double(words(27)),
      double(words(28)),
      double(words(29)),
      double(words(30)),
      double(words(31)),
      double(words(32)),
      double(words(33))
    ))
  }
}
