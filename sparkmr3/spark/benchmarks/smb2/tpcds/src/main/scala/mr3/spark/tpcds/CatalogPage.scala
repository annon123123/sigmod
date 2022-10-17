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

case object CatalogPage extends TPCDS {
  val schema = StructType(
    StructField("cp_catalog_page_sk", LongType) ::
      StructField("cp_catalog_page_id", StringType) ::
      StructField("cp_start_date_sk", LongType) ::
      StructField("cp_end_date_sk", LongType) ::
      StructField("cp_department", StringType) ::
      StructField("cp_catalog_number", IntegerType) ::
      StructField("cp_catalog_page_number", IntegerType) ::
      StructField("cp_description", StringType) ::
      StructField("cp_type", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      long(words(2)),
      long(words(3)),
      string(words(4)),
      int(words(5)),
      int(words(6)),
      string(words(7)),
      string(words(8))
    ))
  }
}
