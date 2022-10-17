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

case object ShipMode extends TPCDS {
  val schema = StructType(
    StructField("sm_ship_mode_sk", LongType) ::
      StructField("sm_ship_mode_id", StringType) ::
      StructField("sm_type", StringType) ::
      StructField("sm_code", StringType) ::
      StructField("sm_carrier", StringType) ::
      StructField("sm_contract", StringType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      string(words(3)),
      string(words(4)),
      string(words(5))
    ))
  }
}
