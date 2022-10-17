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

case object CatalogSales extends TPCDS {
  val schema = StructType(
    StructField("cs_sold_date_sk", LongType) ::
      StructField("cs_sold_time_sk", LongType) ::
      StructField("cs_ship_date_sk", LongType) ::
      StructField("cs_bill_customer_sk", LongType) ::
      StructField("cs_bill_cdemo_sk", LongType) ::
      StructField("cs_bill_hdemo_sk", LongType) ::
      StructField("cs_bill_addr_sk", LongType) ::
      StructField("cs_ship_customer_sk", LongType) ::
      StructField("cs_ship_cdemo_sk", LongType) ::
      StructField("cs_ship_hdemo_sk", LongType) ::
      StructField("cs_ship_addr_sk", LongType) ::
      StructField("cs_call_center_sk", LongType) ::
      StructField("cs_catalog_page_sk", LongType) ::
      StructField("cs_ship_mode_sk", LongType) ::
      StructField("cs_warehouse_sk", LongType) ::
      StructField("cs_item_sk", LongType) ::
      StructField("cs_promo_sk", LongType) ::
      StructField("cs_order_number", LongType) ::
      StructField("cs_quantity", IntegerType) ::
      StructField("cs_wholesale_cost", DoubleType) ::
      StructField("cs_list_price", DoubleType) ::
      StructField("cs_sales_price", DoubleType) ::
      StructField("cs_ext_discount_amt", DoubleType) ::
      StructField("cs_ext_sales_price", DoubleType) ::
      StructField("cs_ext_wholesale_cost", DoubleType) ::
      StructField("cs_ext_list_price", DoubleType) ::
      StructField("cs_ext_tax", DoubleType) ::
      StructField("cs_coupon_amt", DoubleType) ::
      StructField("cs_ext_ship_cost", DoubleType) ::
      StructField("cs_net_paid", DoubleType) ::
      StructField("cs_net_paid_inc_tax", DoubleType) ::
      StructField("cs_net_paid_inc_ship", DoubleType) ::
      StructField("cs_net_paid_inc_ship_tax", DoubleType) ::
      StructField("cs_net_profit", DoubleType) :: Nil
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
