## Table of contents

Hive-MR3:
* [Building Hive-MR3](#building-hive-mr3)
* [Running TPC-DS queries using Hive-MR3](#running-tpc-ds-queries-using-hive-mr3)

Spark-MR3:
* [Preparing Spark-MR3](#preparing-spark-mr3)
* [Running Spark multi-user benchmark using Spark-MR3](#running-spark-multi-user-benchmark-using-spark-mr3)
* [Running Spark-MR3](#running-spark-mr3)

## Building Hive-MR3

Clone this repository and open `hivemr3/env.sh`.
Set `TEZ_SRC` and `HIVE3_SRC` in `hivemr3/env.sh` to the path of `src/tez-mr3` and `src/hive-mr3`.
```sh
$ vi hivemr3/env.sh

TEZ_SRC=~/tmp/src/tez-mr3
HIVE3_SRC=~/tmp/src/hive-mr3
```

Firstly, run `hivemr3/tez/compile-tez.sh`.
```sh
$ hivemr3/tez/compile-tez.sh

# Compiling Tez-MR3 (tez-mr3, 0.9.1.mr3.1.0) #
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] --------------------------------[ pom ]---------------------------------

...

hadoop-mapreduce-client-common-3.1.2.jar  kerb-simplekdc-1.0.1.jar		 stax-api-1.0.1.jar
hadoop-mapreduce-client-core-3.1.2.jar	  kerb-util-1.0.1.jar			 token-provider-1.0.1.jar
hadoop-yarn-api-3.1.2.jar		  kerby-asn1-1.0.1.jar			 woodstox-core-5.0.3.jar
hadoop-yarn-client-3.1.2.jar		  kerby-config-1.0.1.jar
hadoop-yarn-common-3.1.2.jar		  kerby-pkix-1.0.1.jar

Compilation succeeded
```

Then run `hivemr3/hive/compile-hive.sh`.
```sh
$ hivemr3/hive/compile-hive.sh

# Compiling Hive-MR3 (hive-mr3, 3.1.3) #
Refreshing jar /tmp/hivemr3/mr3/mr3jar/mr3-core-1.0.jar in maven repo...
[INFO] Scanning for projects...
[INFO] 

...

[INFO] Hive Packaging ..................................... SUCCESS [ 46.679 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  04:53 min
[INFO] Finished at: 2022-10-17T13:00:00+09:00
[INFO] ------------------------------------------------------------------------

Command: mvn clean install -Dmaven.javadoc.skip=true -Pdist -Dtez.version=0.9.1.mr3.1.0 -DskipTests  

/tmp/src/hive-mr3/itests /tmp/hivemr3

Compilation succeeded
```

## Running TPC-DS queries using Hive-MR3

You can run TPC-DS queries using Hive-MR3 on [a local machine](doc/run-hive-mr3-local.md) or [Yarn](doc/run-hive-mr3-cluster.md).
Check the linked documents to see how to run TPC-DS queries on a selected environment.

## Preparing Spark-MR3

You don't have to build any binary files to run Spark-MR3 batch mode.

* If you want to know how to build Spark-MR3 batch mode, see [this document](doc/build-spark-batch.md).
* If you want to run Spark-MR3 pipelined mode, see [this document](doc/build-spark-pipelined.md).

## Running Spark multi-user benchmark using Spark-MR3

### Configuraing Spark-MR3

Open `sparkmr3/env.sh` and set the following environment variables.

```sh
export JAVA_HOME=~/bin/java8
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_HOME=~/bin/hadoop-3.1.1

SPARK_JARS_DIR=~/tmp/spark-3.2.2-bin-hadoop3.2/jars
export SPARK_HOME=~/tmp/spark-3.2.2-bin-hadoop3.2
```

In order to adjust the resource per each task or worker, open `spark/mr3/conf/local/spark/spark-defaults.conf` and set the following configuration keys. (Use `spark/mr3/conf/cluster/spark/spark-defaults.conf` when running Spark-MR3 on Yarn cluster.)

```sh
spark.driver.cores=4
spark.driver.memory=4g

spark.executor.cores=4
spark.executor.memory=3g
spark.executor.memoryOverhead=1g

spark.task.cpus=1
```

The size of each worker is determined by sum of `spark.executor.memory` and `spark.executor.memoryOverhead`, but the heap size of JVM will bound by `spark.executor.memory`.

### Running Spark Multi-user benchmark using Spark-MR3

The script for runnnig Spark Multi-user benchmark(SMB2) is `sparkmr3/spark/benchmarks/smb2/run-tpcds.sh`.
You should specify the path of text format TPC-DS dataset using option `--db_path`. (See Generating TPC-DS dataset section to generate TPC-DS dataset.)
For example, you can run SMB2 using the below command.
```sh
sparkmr3/spark/benchmarks/smb2/run-tpcds.sh --mode mr3 ---local -db_path file:///tmp/tpcds-generate/2 --query 19,42,52,55,63,68,73,98
```

Use `--cluster` option instead of `--local` when you run SMB2 on Yarn cluster.

## Running Spark-MR3

You can run Spark-Shell and Spark-Submit using Spark-MR3 using `sparkmr3/spark/run-spark-shell.sh` and `sparkmr3/spark/run-spark-submit.sh`.
Below commands show examples of running Spark-Shell and Spark-Submit using Spark-MR3.
```sh
sparkmr3//spark/run-spark-shell.sh --local
sparkmr3/spark/run-spark-submit.sh --local --class WordCount sparkmr3/spark/benchmarks/wordcount/target/scala-2.12/simple-job_2.12-0.1.jar 4 4 a.txt,b.txt
```

This repository includes a wordcount job that runs on Spark under `sparkmr3/spark/benchmarks/wordcount`.
In order to run this job as the above example, move to `sparkmr3/spark/benchmarks/wordcount` and compile the source code using sbt.
```sh
$ sbt
sbt:simple-job> package
[success] Total time: 1 s, completed Oct 14, 2022 1:05:16 PM
sbt:simple-job> 
```

The wordcount job needs 3 arguments.
* First argument is the number of tasks for warming up workers.
* Second argument is the number of reducer tasks.
* Third argument is the path of input files separated by comma.

