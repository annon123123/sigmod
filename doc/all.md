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
## Running TPC-DS queries using Hive-MR3 on a local machine

This sections explains how to generate TPC-DS dataset and run TPC-DS queries using Hive-MR3 on a local machine.

### Configuring Hive-MR3

Clone this repository and open `hivemr3/env.sh`.
Set `JAVA_HOME` and `HADOOP_HOME` to the installation directory of Java and Hadoop respectively.

```sh
export JAVA_HOME=~/bin/java8
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_HOME=~/bin/hadoop-3.1.1
```

Set the following environment variables in `hivemr3/env.sh` to adjust the memory size (in MB) to be allocated to each component:

* `HIVE_METASTORE_HEAPSIZE` specifies the memory size for Metastore.
* `HIVE_SERVER2_HEAPSIZE` specifies the memory size for HiveServer2.
* `HIVE_CLIENT_HEAPSIZE` specifies the memory size of HiveCLI (`hive` command) and Beeline (`beeline` command).
* `MR3_AM_HEAPSIZE` specifies the memory size of MR3 DAGAppMaster.

In order to adjust the resource per each task or worker, open `hivemr3/conf/local/hive3/hive-site.xml` and set the following configuration keys.

```xml
<property>
  <name>hive.mr3.map.task.memory.mb</name>
  <value>2048</value>
</property>

<property>
  <name>hive.mr3.map.task.vcores</name>
  <value>1</value>
</property>

<property>
  <name>hive.mr3.reduce.task.memory.mb</name>
  <value>2048</value>
</property>

<property>
  <name>hive.mr3.reduce.task.vcores</name>
  <value>1</value>
</property>

<property>
  <name>hive.mr3.all-in-one.containergroup.memory.mb</name>
  <value>8192</value>
</property>

<property>
  <name>hive.mr3.all-in-one.containergroup.vcores</name>
  <value>4</value>
</property>
```

When updating these configuration keys, one should meet the following requirements:

* `hive.mr3.map.task.memory.mb` <= `hive.mr3.all-in-one.containergroup.memory.mb`
* `hive.mr3.map.task.vcores` <= `hive.mr3.all-in-one.containergroup.vcores`
* `hive.mr3.reduce.task.memory.mb` <= `hive.mr3.all-in-one.containergroup.memory.mb`
* `hive.mr3.reduce.task.vcores` <= `hive.mr3.all-in-one.containergroup.vcores`

Check `hive.exec.scratchdir` in `hivemr3/conf/local/hive3/hive-site.xml` and make sure that the specified directory does not exist or its permission is set to 733.
```sh
$ vi hivemr3/conf/local/hive3/hive-site.xml
<property>
  <name>hive.exec.scratchdir</name>
  <value>/tmp/hive</value>
</property>

$ ls -al /tmp/hive
total 28
drwx-wx-wx  3 user user  4096 Oct 17 11:46 .
```

### Running Metastore

The script for running Metastore is `hivemr3/hive/metastore-service.sh`. You should initialize Metastore using `--init-schema` option. Don't use `--init-schema` option when you reuse the initialized Hive database. For example, you can run Metastore on your local machine as follows:
```sh
$ hivemr3/hive/metastore-service.sh start --local --init-schema

# Running Metastore using Hive-MR3 (3.1.3) #

Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3--2022-10-17-11-16-17-41031659

Starting Metastore...
Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3--2022-10-17-11-16-17-41031659
```

To stop Metastore use the following command.
```sh
$ hivemr3/hive/metastore-service.sh stop --local

# Running Metastore using Hive-MR3 (3.1.3) #

Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3--2022-10-17-11-47-04-825044e1

Stopping Metastore...
Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3--2022-10-17-11-47-04-825044e1
```

### Running HiveServer2

The script for running HiveServer2 is `hivemr3/hive/hiveserver2-service.sh`.
For example, you can start and stop HiveServer2 on your local machine as follows:
```sh
$ hivemr3/hive/hiveserver2-service.sh start --local

# Running HiveServer2 using Hive-MR3 (3.1.3, 0.9.1.mr3.1.0) #

Output Directory: 
/tmp/hivemr3/hive/hiveserver2-service-result/hive-mr3--2022-10-17-11-16-33-9ca98804

Starting HiveServer2...


$ hivemr3/hive/hiveserver2-service.sh stop --local

# Running HiveServer2 using Hive-MR3 (3.1.3, 0.9.1.mr3.1.0) #

Output Directory: 
/tmp/hivemr3/hive/hiveserver2-service-result/hive-mr3--2022-10-17-11-46-59-a08661ef

Stopping HiveServer2...
```

### Generating TPC-DS dataset

Check `HIVE_DS_FORMAT` and `HIVE_DS_SCALE_FACTOR` in `hivemr3/env.sh`.
* `HIVE_DS_FORMAT` can be either orc, textfile, or rcfile.
* `HIVE_DS_SCALE_FACTOR` determines the overall size of dataset (in GB).

You can generate TPC-DS dataset by running `hivemr3/hive/gen-tpcds.sh`.
```sh
$ hivemr3/hive/gen-tpcds.sh --local

# Generating data using Hive-MR3 (3.1.3, 0.9.1.mr3.1.0) #

Output Directory: 
/tmp/hivemr3/hive/gen-tpcds-result/hive-mr3--2022-10-17-12-00-35-23fe2402

Job Status, Error Status, Hostname, Start Time, Runtime (seconds), Job Name, Run Number, Directory
```

### Running TPC-DS queries

Run Beeline as below example to submit TPC-DS queries.
```sh
hivemr3/hive/run-beeline.sh --local
```

Choose the generated TPC-DS database.
```sh
0: jdbc:hive2://Ubuntu18LAB:9832/> show databases;
+------------------------------+
|        database_name         |
+------------------------------+
| default                      |
| tpcds_bin_partitioned_orc_2  |
| tpcds_text_2                 |
+------------------------------+
3 rows selected (1.646 seconds)
0: jdbc:hive2://Ubuntu18LAB:9832/> use tpcds_bin_partitioned_orc_2;
No rows affected (0.058 seconds)
0: jdbc:hive2://Ubuntu18LAB:9832/> 
```

TPC-DS queries are stored under `hivemr3/hive/benchmarks/hive-testbench/sample-queries-tpcds-hive2`.
You can run these queries as follows:
```sh
0: jdbc:hive2://Ubuntu18LAB:9832/> !run /tmp/hivemr3/hive/benchmarks/hive-testbench/sample-queries-tpcds-hive2/query4.sql
>>>  -- start query 1 in stream 0 using template query4.tpl and seed 1819994127
>>>  with year_total as (
select c_customer_id customer_id
,c_first_name customer_first_name
...
INFO  : Task Execution Summary
INFO  : --------------------------------------------------------------------------------------------------------------------------------
INFO  :   VERTICES  TOTAL_TASKS  FAILED_ATTEMPTS  KILLED_TASKS  INPUT_RECORDS  OUTPUT_RECORDS
INFO  : --------------------------------------------------------------------------------------------------------------------------------
INFO  :      Map 1            1                0             0      1,153,778          22,090
INFO  :     Map 11            3                0             0      1,964,230          81,485
INFO  :     Map 14            1                0             0         10,000           2,190
INFO  :     Map 15            1                0             0        144,000         864,000
INFO  :      Map 3            3                0             0      1,959,837          81,209
INFO  :      Map 5            1                0             0      1,435,602          51,441
INFO  :      Map 7            1                0             0      1,156,513          22,299
INFO  :      Map 9            1                0             0      1,435,282          51,392
INFO  : Reducer 10            5                0             0         51,392          51,392
INFO  : Reducer 12            5                0             0        296,131              31
INFO  : Reducer 13            1                0             0             31               0
INFO  :  Reducer 2            6                0             0         22,090          22,090
INFO  :  Reducer 4            5                0             0         81,209          69,732
INFO  :  Reducer 6            5                0             0         51,441          49,560
INFO  :  Reducer 8            6                0             0         22,299          21,872
INFO  : --------------------------------------------------------------------------------------------------------------------------------
INFO  : 
INFO  : Completed executing command(queryId=20221017113003_33962c1e-60a2-4a2b-afd1-3fe9fd3c044c); Time taken: 16.041 seconds
+-------------------------------------------+
| t_s_secyear.customer_preferred_cust_flag  |
+-------------------------------------------+
| N                                         |
| N                                         |
| N                                         |
...
| Y                                         |
| Y                                         |
| Y                                         |
+-------------------------------------------+
31 rows selected (32.106 seconds)
>>>  
>>>  -- end query 1 in stream 0 using template query4.tpl
0: jdbc:hive2://Ubuntu18LAB:9832/> 
```

## Running TPC-DS queries using Hive-MR3 on Yarn

This sections explains how to generate TPC-DS dataset and run TPC-DS queries using Hive-MR3 on Yarn.

### Configuring Hive-MR3

Clone this repository and open `hivemr3/env.sh`.
Set `JAVA_HOME` and `HADOOP_HOME` to the installation directory of Java and Hadoop respectively.

```sh
export JAVA_HOME=~/bin/java8
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_HOME=~/bin/hadoop-3.1.1
```

Set the following environment variables in `hivemr3/env.sh` to adjust the memory size (in MB) to be allocated to each component:

* `HIVE_METASTORE_HEAPSIZE` specifies the memory size for Metastore.
* `HIVE_SERVER2_HEAPSIZE` specifies the memory size for HiveServer2.
* `HIVE_CLIENT_HEAPSIZE` specifies the memory size of HiveCLI (`hive` command) and Beeline (`beeline` command).
* `MR3_AM_HEAPSIZE` specifies the memory size of MR3 DAGAppMaster.

In order to adjust the resource per each task or worker, open `hivemr3/conf/cluster/hive3/hive-site.xml` and set the following configuration keys.

```xml
<property>
  <name>hive.mr3.map.task.memory.mb</name>
  <value>4096</value>
</property>

<property>
  <name>hive.mr3.map.task.vcores</name>
  <value>1</value>
</property>

<property>
  <name>hive.mr3.reduce.task.memory.mb</name>
  <value>4096</value>
</property>

<property>
  <name>hive.mr3.reduce.task.vcores</name>
  <value>1</value>
</property>

<property>
  <name>hive.mr3.all-in-one.containergroup.memory.mb</name>
  <value>40960</value>
</property>

<property>
  <name>hive.mr3.all-in-one.containergroup.vcores</name>
  <value>10</value>
</property>
```

When updating these configuration keys, one should meet the following requirements:

* `hive.mr3.map.task.memory.mb` <= `hive.mr3.all-in-one.containergroup.memory.mb`
* `hive.mr3.map.task.vcores` <= `hive.mr3.all-in-one.containergroup.vcores`
* `hive.mr3.reduce.task.memory.mb` <= `hive.mr3.all-in-one.containergroup.memory.mb`
* `hive.mr3.reduce.task.vcores` <= `hive.mr3.all-in-one.containergroup.vcores`

Open `hivemr3/conf/cluster/hive3/hive-site.xml` and change the following configuration key to point the `hive-llap-common-3.1.3.jar`, `hive-llap-server-3.1.3.jar`, and `hive-llap-tez-3.1.3.jar` under `hivemr3/hive/hivejar/apache-hive-3.1.3-bin/lib`.

```xml
<property>
  <name>hive.aux.jars.path</name>
  <value>~/hivemr3/hive/hivejar/apache-hive-3.1.3-bin/lib/hive-llap-common-3.1.3.jar,~/hivemr3/hive/hivejar/apache-hive-3.1.3-bin/lib/hive-llap-server-3.1.3.jar,~/hivemr3/hive/hivejar/apache-hive-3.1.3-bin/lib/hive-llap-tez-3.1.3.jar</value>
</property>
```

Check whether `hive.exec.scratchdir` (specified in `hivemr3/conf/cluster/hive3/hive-site.xml`) exists in HDFS or not.
If the directory exists, then its permission should be 733.
```sh
$ vi hivemr3/conf/cluster/hive3/hive-site.xml
<property>
  <name>hive.exec.scratchdir</name>
  <value>/tmp/hive</value>
</property>

$ hdfs dfs -ls /tmp | grep hive
drwx-wx-wx   - gitlab-runner hdfs          0 2021-10-29 11:09 /tmp/hive
```

Check `HDFS_LIB_DIR` and `HIVE3_HDFS_WAREHOUSE` in `hivemr3/env.sh` and create the specified directories if they do not exist.
```sh
$ vi hivemr3/env.sh
HDFS_LIB_DIR=/user/$USER/lib
HIVE3_HDFS_WAREHOUSE=/tmp/hivemr3/warehouse

$ hdfs dfs -ls /tmp/hivemr3
Found 1 items
drwxr-xr-x   - gitlab-runner hdfs          0 2022-10-14 13:41 /tmp/hivemr3/warehouse
$ hdfs dfs -ls /user/gitlab-runner | grep lib
drwxr-xr-x   - gitlab-runner gitlab-runner          0 2022-10-14 13:39 /user/gitlab-runner/lib
```

Load MR3 and Tez jar files using following scripts:
```sh
$ hivemr3/mr3/upload-hdfslib-mr3.sh 

# Uploading mr3 jar files to hdfs #
Output (HDFS): /user/gitlab-runner/lib/mr3

-rw-r--r--   3 gitlab-runner gitlab-runner    7093129 2022-10-17 12:24 /user/gitlab-runner/lib/mr3/mr3-core-1.0.jar
-rw-r--r--   3 gitlab-runner gitlab-runner   30579203 2022-10-17 12:24 /user/gitlab-runner/lib/mr3/mr3-tez-1.0-assembly.jar
-rw-r--r--   3 gitlab-runner gitlab-runner     293700 2022-10-17 12:24 /user/gitlab-runner/lib/mr3/mr3-tez-1.0.jar
$ hivemr3/tez/upload-hdfslib-tez.sh 

# Uploading tez-0.9.1.mr3.1.0 jar files to hdfs #
Output (HDFS): /user/gitlab-runner/lib/tez

drwxr-xr-x   - gitlab-runner gitlab-runner          0 2022-10-17 12:24 /user/gitlab-runner/lib/tez/tar
-rw-r--r--   3 gitlab-runner gitlab-runner   41771978 2022-10-17 12:24 /user/gitlab-runner/lib/tez/tar/tez-0.9.1.mr3.1.0.tar.gz
```

### Running Metastore

The script for running Metastore is `hivemr3/hive/metastore-service.sh`. You should initialize Metastore using `--init-schema` option as following example.
```sh
$ hivemr3/hive/metastore-service.sh start --cluster --init-schema

# Running Metastore using Hive-MR3 (3.1.3) #

Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3-bcc431b-2022-10-17-12-32-43-b7ff664c

Starting Metastore...
Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3-bcc431b-2022-10-17-12-32-43-b7ff664c
```
Don't use `--init-schema` option when you reuse the initialized Hive database.

To stop Metastore use the following command.
```sh
$ hivemr3/hive/metastore-service.sh stop --cluster
# Running Metastore using Hive-MR3 (3.1.3) #

Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3-bcc431b-2022-10-17-12-40-46-0900b5dd

Stopping Metastore...
Output Directory: 
/tmp/hivemr3/hive/metastore-service-result/hive-mr3-bcc431b-2022-10-17-12-40-46-0900b5dd
```

### Running HiveServer2

The script for running HiveServer2 is `hivemr3/hive/hiveserver2-service.sh`.
For example, you can start and stop HiveServer2 as follows:
```sh
$ hivemr3/hive/hiveserver2-service.sh start --cluster
# Running HiveServer2 using Hive-MR3 (3.1.3, 0.9.1.mr3.1.0) #

Output Directory: 
/tmp/hivemr3/hive/hiveserver2-service-result/hive-mr3-bcc431b-2022-10-17-12-33-43-5c8c7731

Starting HiveServer2...

$ hivemr3/hive/hiveserver2-service.sh stop --cluster

# Running HiveServer2 using Hive-MR3 (3.1.3, 0.9.1.mr3.1.0) #

Output Directory: 
/tmp/hivemr3/hive/hiveserver2-service-result/hive-mr3-bcc431b-2022-10-17-12-40-33-d0cea5f3

Stopping HiveServer2...
```

### Generating TPC-DS dataset

Check `HIVE_DS_FORMAT` and `HIVE_DS_SCALE_FACTOR` in `hivemr3/env.sh`.
* `HIVE_DS_FORMAT` can be either orc, textfile, or rcfile.
* `HIVE_DS_SCALE_FACTOR` determines the overall size of dataset (in GB).

You can generate TPC-DS dataset by running `hivemr3/hive/gen-tpcds.sh`.
```sh
hivemr3/hive/gen-tpcds.sh --cluster
```

### Running TPC-DS queries

Run Beeline as below example to submit TPC-DS queries.
```sh
hivemr3/hive/run-beeline.sh --cluster
```

Choose the generated TPC-DS database.
```sh
0: jdbc:hive2://master:9832/> show databases;
+------------------------------+
|        database_name         |
+------------------------------+
| default                      |
| tpcds_bin_partitioned_orc_2  |
| tpcds_text_2                 |
+------------------------------+
3 rows selected (1.646 seconds)
0: jdbc:hive2://master:9832/> use tpcds_bin_partitioned_orc_2;
No rows affected (0.058 seconds)
0: jdbc:hive2://master:9832/> 
```

TPC-DS queries are stored under `hivemr3/hive/benchmarks/hive-testbench/sample-queries-tpcds-hive2`.
You can run these queries as follows:
```sh
0: jdbc:hive2://master:9832/> !run /tmp/hivemr3/hive/benchmarks/hive-testbench/sample-queries-tpcds-hive2/query2.sql
...
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
+--------------+-------+-------+-------+-------+-------+-------+-------+
| d_week_seq1  |  _c1  |  _c2  |  _c3  |  _c4  |  _c5  |  _c6  |  _c7  |
+--------------+-------+-------+-------+-------+-------+-------+-------+
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
| 5321         | 0.99  | 1.07  | 0.95  | 1.12  | 1.04  | 1.04  | 1.03  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
| 5322         | 5.36  | 4.98  | 0.96  | 2.0   | 1.87  | 5.36  | 4.86  |
+--------------+-------+-------+-------+-------+-------+-------+-------+
2,513 rows selected (16.813 seconds)
>>>  
>>>  -- end query 1 in stream 0 using template query2.tpl
0: jdbc:hive2://master:9832/> 
```

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

