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

case object CatalogReturns extends TPCDS {
  val schema = StructType(
    StructField("cr_returned_date_sk", LongType) ::
      StructField("cr_returned_time_sk", LongType) ::
      StructField("cr_item_sk", LongType) ::
      StructField("cr_refunded_customer_sk", LongType) ::
      StructField("cr_refunded_cdemo_sk", LongType) ::
      StructField("cr_refunded_hdemo_sk", LongType) ::
      StructField("cr_refunded_addr_sk", LongType) ::
      StructField("cr_returning_customer_sk", LongType) ::
      StructField("cr_returning_cdemo_sk", LongType) ::
      StructField("cr_returning_hdemo_sk", LongType) ::
      StructField("cr_returning_addr_sk", LongType) ::
      StructField("cr_call_center_sk", LongType) ::
      StructField("cr_catalog_page_sk", LongType) ::
      StructField("cr_ship_mode_sk", LongType) ::
      StructField("cr_warehouse_sk", LongType) ::
      StructField("cr_reason_sk", LongType) ::
      StructField("cr_order_number", LongType) ::
      StructField("cr_return_quantity", IntegerType) ::
      StructField("cr_return_amount", DoubleType) ::
      StructField("cr_return_tax", DoubleType) ::
      StructField("cr_return_amt_inc_tax", DoubleType) ::
      StructField("cr_fee", DoubleType) ::
      StructField("cr_return_ship_cost", DoubleType) ::
      StructField("cr_refunded_cash", DoubleType) ::
      StructField("cr_reversed_charge", DoubleType) ::
      StructField("cr_store_credit", DoubleType) ::
      StructField("cr_net_loss", DoubleType) :: Nil
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
      int(words(17)),
      double(words(18)),
      double(words(19)),
      double(words(20)),
      double(words(21)),
      double(words(22)),
      double(words(23)),
      double(words(24)),
      double(words(25)),
      double(words(26))
    ))
  }
}
