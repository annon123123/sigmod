import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.fs.Path
import scala.collection.mutable
import org.apache.spark.{Partitioner, SparkContext}
import scala.util.Random
import org.apache.hadoop.conf.Configuration

object WordCount {
  def run(input: Seq[String], numReducer: Int, sc: SparkContext): Long = {
    val map = sc.parallelize(input, input.length)
    val tokens = map.flatMap { f =>
      val path = new Path(f)
      val fs = path.getFileSystem(new Configuration())
      val in = fs.open(path)
      val reader = new BufferedReader(new InputStreamReader(in))

      var result: Iterator[(String, Int)] = Iterator.empty

      var line = reader.readLine()
      while (line != null) {
        val itr = line.split(" ").map(x => (x, 1)).toIterator
        result = result ++ itr

        line = reader.readLine()
      }

      result
    }

    val part = new Partitioner {
      def numPartitions: Int = numReducer

      def getPartition(key: Any): Int = {
        val x = key.asInstanceOf[String].hashCode % numReducer
        if (x < 0) x + numReducer else x
      }
    }

    val shuffled = tokens.partitionBy(part)
    val maps = shuffled.mapPartitions { itr =>
      val map = new mutable.HashMap[String, Int]

      itr.foreach { case (x, _) =>
        val c = map.getOrElse(x, 1)
        map(x) = c + 1
      }

      Iterator(map)
    }

    val start = System.currentTimeMillis()
    maps.collect()
    val end = System.currentTimeMillis()

    end - start
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      return
    }
    
    val numWarmupTasks = args(0).toInt
    val numReducer = args(1).toInt
    val paths = args(2).split(",")

    val sc = new SparkContext

    sc.parallelize(0 until numWarmupTasks, numWarmupTasks)
      .map { _ =>
        Thread.sleep(10 * 1000)
        Random.nextInt
      }
      .collect()
    
    val duration = run(paths, numReducer, sc)
    sc.stop()
    println(duration)
    System.exit(0)
  }
}

