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

trait TPCDS {
  protected def string(str: String): String = str.trim

  protected def int(str: String): Int = {
    if (str.trim == null || (str.trim == "")) 0 else str.trim.toInt
  }

  protected def long(str: String): Long = {
    if (str.trim == null || (str.trim == "")) 0L else str.trim.toLong
  }

  protected def double(str: String): Double = {
    if (str.trim == null || (str.trim == "")) 0.0 else str.trim.toDouble
  }

  def schema: StructType
  def parseRDD(rdd: RDD[Array[String]]): RDD[Row]
}

object TPCDS {
  val tables = Map(
    "call_center" -> CallCenter,
    "catalog_page" -> CatalogPage,
    "catalog_returns" -> CatalogReturns,
    "catalog_sales" -> CatalogSales,
    "customer" -> Customer,
    "customer_address" -> CustomerAddress,
    "customer_demographics" -> CustomerDemographics,
    "date_dim" -> DateDim,
    "household_demographics" -> HouseholdDemographics,
    "income_band" -> IncomeBand,
    "inventory" -> Inventory,
    "item" -> Item,
    "promotion" -> Promotion,
    "reason" -> Reason,
    "ship_mode" -> ShipMode,
    "store" -> Store,
    "store_returns" -> StoreReturns,
    "store_sales" -> StoreSales,
    "time_dim" -> TimeDim,
    "warehouse" -> Warehouse,
    "web_page" -> WebPage,
    "web_returns" -> WebReturns,
    "web_sales" -> WebSales,
    "web_site" -> WebSite
  )

  def splitRDD(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(_.split("\\|", -1))
  }
}
