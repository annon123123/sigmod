## Preparing Spark-MR3 pipelined mode

### Building Spark-MR3 pipelined mode

To run Spark-MR3 pipelined mode, you have to compile modified Spark and Spark-MR3.

Open `sparkmr3/env.sh` and set the following environment variables.
* Set `SPARK_MR3_SRC` to the path of `src/spark-pipelined-mode/spark-mr3`.
* Set `SPARK_HOME` to the path of `src/spark-pipelined-mode/spark`.
* Set `SPARK_JARS_DIR` to the path of `src/spark-pipelined-mode/spark/assembly/target/scala-2.12/jars`.

```sh
$ vi sparkmr3/env.sh

SPARK_MR3_SRC=/tmp/src/spark-pipelined-mode/spark-mr3
SPARK_JARS_DIR=/tmp/src/spark-pipelined-mode/spark/assembly/target/scala-2.12/jars
export SPARK_HOME=/tmp/src/spark-pipelined-mode/spark
```

Move to `src/spark-pipelined-mode/spark` and run `compile.sh`.
```sh
$ cd src/spark-pipelined-mode/spark
$ ./compile.sh
Using `mvn` from path: /usr/bin/mvn
[INFO] Scanning for projects...
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Build Order:
[INFO] 
[INFO] Spark Project Parent POM                                           [pom]
[INFO] Spark Project Tags                                                 [jar]
[INFO] Spark Project Sketch                                               [jar]

...

[INFO] Spark Integration for Kafka 0.10 ................... SKIPPED
[INFO] Kafka 0.10+ Source for Structured Streaming ........ SKIPPED
[INFO] Spark Project Examples ............................. SKIPPED
[INFO] Spark Integration for Kafka 0.10 Assembly .......... SKIPPED
[INFO] Spark Avro ......................................... SKIPPED
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  1.347 s
[INFO] Finished at: 2022-10-17T13:46:37+09:00
[INFO] ------------------------------------------------------------------------
```

Run `sparkmr3/spark/compile-spark.sh`.

### Configuring Spark-MR3 pipelined mode

Open `sparkmr3/conf/local/spark/spark-defaults.conf` and set `spark.shuffle.manager` to `org.apache.spark.shuffle.MR3ShuffleManager`.
```sh
$ vi sparkmr3/conf/local/spark/spark-defaults.conf

spark.shuffle.manager=org.apache.spark.shuffle.MR3ShuffleManager
```
Copy `src/spark-pipelined-mode/mr3-core-1.0.jar` and `src/spark-pipelined-mode/mr3-spark-1.0.jar` to `sparkmr3/mr3/mr3jar`.
Copy `src/spark-pipelined-mode/mr3-spark-1.0-assembly.jar` to `sparkmr3/mr3/mr3lib/mr3-spark-1.0-assembly.jar`.
```sh
cp src/spark-pipelined-mode/mr3-core-1.0.jar sparkmr3/mr3/mr3jar
cp src/spark-pipelined-mode/mr3-spark-1.0.jar sparkmr3/mr3/mr3jar
cp src/spark-pipelined-mode/mr3-spark-1.0-assembly.jar sparkmr3/mr3/mr3lib
```
