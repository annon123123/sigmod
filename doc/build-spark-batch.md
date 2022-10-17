## Building Spark-MR3 batch mode

Clone this repository and open `sparkmr3/env.sh`.
Set `SPARK_MR3_SRC` in  `sparkmr3/env.sh` to the path of `src/spark-batch-mode/spark-mr3`.
```sh
$ vi sparkmr3/env.sh

SPARK_MR3_SRC=~/tmp/src/spark-batch-mode/spark-mr3
```

Run `sparkmr3/spark/compile-spark.sh`.
```sh
$ sparkmr3/spark/compile-spark.sh

# Compiling Spark-MR3 (spark-mr3, 3.2.2) #
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1

...

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  2.933 s
[INFO] Finished at: 2022-10-17T13:06:26+09:00
[INFO] ------------------------------------------------------------------------
Output: /tmp/sparkmr3/spark/sparkjar

/tmp/sparkmr3/spark/sparkjar:
hadoop-aws-3.2.3.jar  spark-hadoop-cloud_2.12-3.3.0.jar  sparkmr3

/tmp/sparkmr3/spark/sparkjar/sparkmr3:
spark-mr3-3.2.2-assembly.jar

Compilation succeeded
```
