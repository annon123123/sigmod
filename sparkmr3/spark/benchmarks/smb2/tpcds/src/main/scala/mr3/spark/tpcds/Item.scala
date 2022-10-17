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

case object Item extends TPCDS {
  val schema = StructType(
    StructField("i_item_sk", LongType) ::
      StructField("i_item_id", StringType) ::
      StructField("i_rec_start_date", StringType) ::
      StructField("i_rec_end_date", StringType) ::
      StructField("i_item_desc", StringType) ::
      StructField("i_current_price", DoubleType) ::
      StructField("i_wholesale_cost", DoubleType) ::
      StructField("i_brand_id", IntegerType) ::
      StructField("i_brand", StringType) ::
      StructField("i_class_id", IntegerType) ::
      StructField("i_class", StringType) ::
      StructField("i_category_id", IntegerType) ::
      StructField("i_category", StringType) ::
      StructField("i_manufact_id", IntegerType) ::
      StructField("i_manufact", StringType) ::
      StructField("i_size", StringType) ::
      StructField("i_formulation", StringType) ::
      StructField("i_color", StringType) ::
      StructField("i_units", StringType) ::
      StructField("i_container", StringType) ::
      StructField("i_manager_id", IntegerType) ::
      StructField("i_product_name", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      string(words(3)),
      string(words(4)),
      double(words(5)),
      double(words(6)),
      int(words(7)),
      string(words(8)),
      int(words(9)),
      string(words(10)),
      int(words(11)),
      string(words(12)),
      int(words(13)),
      string(words(14)),
      string(words(15)),
      string(words(16)),
      string(words(17)),
      string(words(18)),
      string(words(19)),
      int(words(20)),
      string(words(21))
    ))
  }
}
