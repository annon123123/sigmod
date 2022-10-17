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
