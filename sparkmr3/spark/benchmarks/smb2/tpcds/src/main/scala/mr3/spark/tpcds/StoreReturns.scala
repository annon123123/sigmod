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

case object StoreReturns extends TPCDS {
  val schema = StructType(
    StructField("sr_returned_date_sk", LongType) ::
      StructField("sr_return_time_sk", LongType) ::
      StructField("sr_item_sk", LongType) ::
      StructField("sr_customer_sk", LongType) ::
      StructField("sr_cdemo_sk", LongType) ::
      StructField("sr_hdemo_sk", LongType) ::
      StructField("sr_addr_sk", LongType) ::
      StructField("sr_store_sk", LongType) ::
      StructField("sr_reason_sk", LongType) ::
      StructField("sr_ticket_number", LongType) ::
      StructField("sr_return_quantity", IntegerType) ::
      StructField("sr_return_amt", DoubleType) ::
      StructField("sr_return_tax", DoubleType) ::
      StructField("sr_return_amt_inc_tax", DoubleType) ::
      StructField("sr_fee", DoubleType) ::
      StructField("sr_return_ship_cost", DoubleType) ::
      StructField("sr_refunded_cash", DoubleType) ::
      StructField("sr_reversed_charge", DoubleType) ::
      StructField("sr_store_credit", DoubleType) ::
      StructField("sr_net_loss", DoubleType) :: Nil
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
      double(words(19))
    ))
  }
}
