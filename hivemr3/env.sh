#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# All directories and paths are those on the machine where Hive-MR3 is installed unless otherwise noted.

#
# Step 1. JAVA_HOME and PATH
#

# set JAVA_HOME if not set yet 
export JAVA_HOME=~/cdefgab/bin/java8
export PATH=$JAVA_HOME/bin:$PATH

#
# Step 2. Directories
#

# Hadoop home directory in non-local mode
export HADOOP_HOME=~/cdefgab/bin/hadoop-3.1.1

# do not set HADOOP_COMMON_HOME, HADOOP_HDFS_HOME, HADOOP_MAPRED_HOME, HADOOP_YARN_HOME

# HDFS directory where all MR3 and Tez jar files are copied 
# Used only in non-local mode 
HDFS_LIB_DIR=/user/$USER/lib

# Hadoop home directory used for running Hive on MR3 in local mode 
# Hadoop home directory used by build-k8s.sh for copying Hadoop to the Docker image
HADOOP_HOME_LOCAL=$HADOOP_HOME

# Hadoop native library paths, used for MR3 in non-local mode  
HADOOP_NATIVE_LIB=$HADOOP_HOME_LOCAL/lib/native

# fs.defaultFS is determined by HADOOP_HOME or HADOOP_LOCAL_HOME.
# Hence setting fs.defaultFS in hive-site.xml/tez-site.xml/mr3-site.xml has no effect.

#
# Step 3. Tez jar
#

# Specifies whether classes are imported from YarnConfiguration.YARN_APPLICATION_CLASSPATH
# If set to true, the user should ensure the compatibility between Tez-MR3 and classes imported from 
# YarnConfiguration.YARN_APPLICATION_CLASSPATH.
#
# Set to false on vanilla Hadoop clusters, HDP, and CDH
# Set to true on CDH with Kerberos
# Set to true on AWS EMR in order to import EMR-specific classes such as com.amazon.ws.emr.hadoop.fs.EmrFileSystem
#
TEZ_USE_MINIMAL=false

#
# Step 4. Heap size
#

# Heap size in MB for Metastore
HIVE_METASTORE_HEAPSIZE=4096

# Heap size in MB for HiveServer2
HIVE_SERVER2_HEAPSIZE=4096

# Heap size in MB for HiveCLI ('hive' command) and Beeline ('beeline' command) 
HIVE_CLIENT_HEAPSIZE=1024

# Heap size in MB for MR3 DAGAppMaster
# Should be smaller than HIVE_SERVER2_HEAPSIZE in local-thread mode:
#   1) --local option is used in scripts
#   2) mr3.master.mode is set to local-thread in mr3-site.xml
MR3_AM_HEAPSIZE=2048

#
# Step 5. Metastore
#

# HIVE3_DATABASE_HOST = host for Metastore database 
# HIVE3_METASTORE_HOST = host for Metastore itself 
# HIVE3_METASTORE_PORT = port for Hive Metastore in non-local mode 
# HIVE3_METASTORE_LOCAL_PORT= port for Hive Metastore in local mode 
# HIVE3_DATABASE_NAME = database name in Hive Metastore 
# HIVE3_HDFS_WAREHOUSE = HDFS directory for the Hive warehouse in non-local mode 

# Used with --hivesrc3 option in scripts 
HIVE3_DATABASE_HOST=$HOSTNAME
HIVE3_METASTORE_HOST=$HOSTNAME
HIVE3_METASTORE_PORT=9830
HIVE3_METASTORE_LOCAL_PORT=9831
HIVE3_DATABASE_NAME=hive3mr3
HIVE3_HDFS_WAREHOUSE=/user/hive/warehouse

# MySQL connector jar file when --tpcds is used in scripts
HIVE_MYSQL_DRIVER=/usr/share/java/mysql-connector-java.jar

#
# Step 6. HiveServer2
#

# HIVE3_SERVER2_PORT = port for HiveServer2 (for both non-local mode and local mode)
HIVE3_SERVER2_HOST=$HOSTNAME
HIVE3_SERVER2_PORT=9832

#
# Step 7. Security (optional)
#

# Specifies whether the cluster is secure with Kerberos or not 
SECURE_MODE=false

# For running MR3 and Hive-MR3 with SECURE_MODE=true
 
# Kerberos principal for renewing HDFS/Hive tokens
USER_PRINCIPAL=hive@HADOOP
# Kerberos keytab 
USER_KEYTAB=/home/hive/hive.keytab

# Specifies whether HDFS token renewal is enabled inside DAGAppMaster and ContainerWorkers 
TOKEN_RENEWAL_HDFS_ENABLED=false
# Specifies whether Hive token renewal is enabled inside DAGAppMaster and ContainerWorkers 
TOKEN_RENEWAL_HIVE_ENABLED=false

# For security in Metastore 
# Kerberos principal for Metastore; cf. 'hive.metastore.kerberos.principal' in hive-site.xml
HIVE_METASTORE_KERBEROS_PRINCIPAL=hive/_HOST@HADOOP
# Kerberos keytab for Metastore; cf. 'hive.metastore.kerberos.keytab.file' in hive-site.xml
HIVE_METASTORE_KERBEROS_KEYTAB=/etc/security/keytabs/hive.service.keytab

# For security in HiveServer2 
# Authentication option:  NONE (uses plain SASL), NOSASL, KERBEROS, LDAP, PAM, and CUSTOM; cf. 'hive.server2.authentication' in hive-site.xml 
HIVE_SERVER2_AUTHENTICATION=NONE
# Kerberos principal for HiveServer2; cf. 'hive.server2.authentication.kerberos.principal' in hive-site.xml 
HIVE_SERVER2_KERBEROS_PRINCIPAL=hive/_HOST@HADOOP
# Kerberos keytab for HiveServer2; cf. 'hive.server2.authentication.kerberos.keytab' in hive-site.xml 
HIVE_SERVER2_KERBEROS_KEYTAB=/home/hive/hive.keytab

#
# Step 8. Logging (optional)
#

# Logging level 
# TODO: LOG_LEVEL works only partially for Spark on MR3 on Yarn
LOG_LEVEL=INFO

#
# Step 9. For gen-tpcds.sh and run-tpcds.sh (optional)
#

# TPC-DS data format: orc (default), textfile, rcfile 
HIVE_DS_FORMAT=orc
# TPC-DS scale factor in GB 
HIVE_DS_SCALE_FACTOR=2

#
# Step 10. Compilation (optional)
#

# MR3 revision 
MR3_REV=1.0

# Tez-MR3 source directory and its revision number (for compiling Tez-MR3) 
TEZ_SRC=~/tez-mr3
TEZ_REV=0.9.1.mr3.${MR3_REV}

# Hive-MR3 source directory and its revision number (for compiling Hive-MR3)
HIVE3_SRC=~/hive-mr3
HIVE3_REV=3.1.3

#
# Step 11. High availability (optional)
#

# In order to enable high availability, the user should export environment variable MR3_SHARED_SESSION_ID
# before executing hive/hiveserver2-service.sh. Then MR3_SHARED_SESSION_ID is shared by all HiveServer2
# instances. If MR3_SHARED_SESSION_ID is not set, each HiveServer2 instance creates its own MR3 session ID,
# thus never sharing a common MR3 session directory. (Still all HiveServer2 instances share a common MR3
# DAGAppMaster.)

#export MR3_SHARED_SESSION_ID=d7b52f74-7349-405c-88a2-d0d1cbb5a918

#
# Step 12. Choose MR3 runtime
#

MR3_TEZ_ENABLED=true      # for running Hive on MR3
MR3_SPARK_ENABLED=false   # for running Spark on MR3

#
# Miscellaneous
#

# set to true to save all configuration files under kubernetes/
USE_K8S=false

# unset because 'hive' command reads SPARK_HOME and may accidentally expand the classpath with HiveConf.class from Spark. 
unset SPARK_HOME

# disable 'assert{}' when compiling MR3
DISABLE_ASSERT=false   

# Set to true in order to localize MR3/Tez jar files on HDFS specified in HDFS_LIB_DIR
# If set to false, the user should set up the Java classpath so as to include MR3/Tez jar files on each slave machine
LOCALIZE=true   
