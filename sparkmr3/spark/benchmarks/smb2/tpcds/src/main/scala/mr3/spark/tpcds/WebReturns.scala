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

case object WebReturns extends TPCDS {
  val schema = StructType(
    StructField("wr_returned_date_sk", LongType) ::
      StructField("wr_returned_time_sk", LongType) ::
      StructField("wr_item_sk", LongType) ::
      StructField("wr_refunded_customer_sk", LongType) ::
      StructField("wr_refunded_cdemo_sk", LongType) ::
      StructField("wr_refunded_hdemo_sk", LongType) ::
      StructField("wr_refunded_addr_sk", LongType) ::
      StructField("wr_returning_customer_sk", LongType) ::
      StructField("wr_returning_cdemo_sk", LongType) ::
      StructField("wr_returning_hdemo_sk", LongType) ::
      StructField("wr_returning_addr_sk", LongType) ::
      StructField("wr_web_page_sk", LongType) ::
      StructField("wr_reason_sk", LongType) ::
      StructField("wr_order_number", LongType) ::
      StructField("wr_return_quantity", IntegerType) ::
      StructField("wr_return_amt", DoubleType) ::
      StructField("wr_return_tax", DoubleType) ::
      StructField("wr_return_amt_inc_tax", DoubleType) ::
      StructField("wr_fee", DoubleType) ::
      StructField("wr_return_ship_cost", DoubleType) ::
      StructField("wr_refunded_cash", DoubleType) ::
      StructField("wr_reversed_charge", DoubleType) ::
      StructField("wr_account_credit", DoubleType) ::
      StructField("wr_net_loss", DoubleType) :: Nil
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
      int(words(14)),
      double(words(15)),
      double(words(16)),
      double(words(17)),
      double(words(18)),
      double(words(19)),
      double(words(20)),
      double(words(21)),
      double(words(22)),
      double(words(23))
    ))
  }
}
