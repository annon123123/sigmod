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

case object CustomerDemographics extends TPCDS {
  val schema = StructType(
    StructField("cd_demo_sk", LongType) ::
      StructField("cd_gender", StringType) ::
      StructField("cd_marital_status", StringType) ::
      StructField("cd_education_status", StringType) ::
      StructField("cd_purchase_estimate", IntegerType) ::
      StructField("cd_credit_rating", StringType) ::
      StructField("cd_dep_count", IntegerType) ::
      StructField("cd_dep_employed_count", IntegerType) ::
      StructField("cd_dep_college_count", IntegerType) :: Nil
  )

  def parseRDD(rdd: RDD[Array[String]]): RDD[Row] = {
    rdd.map(words => Row(
      long(words(0)),
      string(words(1)),
      string(words(2)),
      string(words(3)),
      int(words(4)),
      string(words(5)),
      int(words(6)),
      int(words(7)),
      int(words(8))
    ))
  }
}
