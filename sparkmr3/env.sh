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
# Step 3. Heap size
#

# Heap size in MB for MR3 DAGAppMaster
MR3_AM_HEAPSIZE=10240

#
# Step 4. Spark Directories
#

#
# For compiling Spark on MR3 
#
SPARK_MR3_SRC=~/spark-mr3
SPARK_MR3_REV=3.2.2

# SPARK_JARS_DIR specifies the directory where Spark jar files are stored
SPARK_JARS_DIR=~/cdefgab/tmp/spark-3.2.2-bin-hadoop3.2/jars
#SPARK_JARS_DIR=~/spark/assembly/target/scala-2.12/jars

# SPARK_HOME must be set to run Spark on MR3 because
# $SPARK_HOME/bin/spark-shell and $SPARK_HOME/bin/spark-submit are executed
export SPARK_HOME=~/cdefgab/tmp/spark-3.2.2-bin-hadoop3.2

#
# Step 5. Security (optional)
#

# Specifies whether the cluster is secure with Kerberos or not 
SECURE_MODE=false

# For running MR3 and Hive-MR3 with SECURE_MODE=true
 
# Kerberos principal for renewing HDFS/Hive tokens
USER_PRINCIPAL=spark@HADOOP
# Kerberos keytab 
USER_KEYTAB=/home/spark/spark.keytab

# Specifies whether HDFS token renewal is enabled inside DAGAppMaster and ContainerWorkers 
TOKEN_RENEWAL_HDFS_ENABLED=false
# Specifies whether Hive token renewal is enabled inside DAGAppMaster and ContainerWorkers 
TOKEN_RENEWAL_HIVE_ENABLED=false

#
# Step 6. Logging (optional)
#

# Logging level 
# TODO: LOG_LEVEL works only partially for Spark on MR3 on Yarn
LOG_LEVEL=INFO

#
# Step 7. Compilation (optional)
#

# MR3 revision 
MR3_REV=1.0

#
# Step 8. Choose MR3 runtime
#

MR3_TEZ_ENABLED=false     # for running Hive on MR3
MR3_SPARK_ENABLED=true    # for running Spark on MR3

#
# Miscellaneous
#

SPARK_DATABASE_HOST=indigo1
SPARK_DATABASE_NAME=hive3mr3
SPARK_HDFS_WAREHOUSE=/tmp/hivemr3/warehouse
SPARK_METASTORE_HOST=localhost
SPARK_METASTORE_PORT=9830
SPARK_THRIFT_SERVER_HOST=localhost
SPARK_THRIFT_SERVER_PORT=9872

# set to true to save all configuration files under kubernetes/
USE_K8S=false

# disable 'assert{}' when compiling MR3
DISABLE_ASSERT=false   

# Set to true in order to localize MR3/Tez jar files on HDFS specified in HDFS_LIB_DIR
# If set to false, the user should set up the Java classpath so as to include MR3/Tez jar files on each slave machine
LOCALIZE=true   

