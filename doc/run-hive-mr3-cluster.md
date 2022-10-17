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
