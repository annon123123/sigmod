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

case object HouseholdDemographics extends TPCDS {
  val schema = StructType(
    StructField("hd_demo_sk", LongType) ::
      StructField("hd_income_band_sk", LongType) ::
      StructField("hd_buy_potential", StringType) ::
      StructField("hd_dep_count", IntegerType) ::
      StructField("hd_vehicle_count", IntegerType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      long(words(1)),
      string(words(2)),
      int(words(3)),
      int(words(4))
    ))
  }
}
