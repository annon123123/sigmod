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

DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR=$(readlink -f $DIR/..)
source $BASE_DIR/env.sh
source $BASE_DIR/common-setup.sh
source $SPARK_BASE_DIR/spark-setup.sh

function upload_hdfslib_spark_print_usage {
    echo "Usage: upload_hdfslib-spark.sh"
    echo " -h                                     Print the usage."
}

function upload_hdfslib_spark_parse_args {
    while [[ -n $1 ]]; do
        case "$1" in
            -h|--help)
                upload_hdfslib_spark_print_usage
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done
}

function upload_hdfslib_spark_main {
    spark_setup_parse_args_common $@
    upload_hdfslib_spark_parse_args $REMAINING_ARGS

    common_setup_init
    spark_setup_init

    COMPILE_OUT_FILE=${COMPILE_OUT_FILE:-$LOG_BASE_DIR/upload-hdfslib-spark.log}

    has_hdfs=$(which hdfs 2>> $COMPILE_OUT_FILE | wc -l)
    if [[ $has_hdfs -gt 0 ]]; then
        hdfs dfs -rm -r $SPARK_HDFS_LIB_DIR/ >> $COMPILE_OUT_FILE 2>&1
        hdfs dfs -mkdir -p $SPARK_HDFS_LIB_DIR/tar >> $COMPILE_OUT_FILE 2>&1

        pushd $SPARK_JARS_DIR
        rm -rf $SPARK_LIB_BASE_DIR/$SPARK_TAR
        tar -czf $SPARK_LIB_BASE_DIR/$SPARK_TAR .
        popd > /dev/null

        echo -e "\n# Uploading spark jar files to hdfs #"
        echo -e "Output (HDFS): $SPARK_HDFS_LIB_DIR\n"
        hdfs dfs -put $SPARK_LIB_BASE_DIR/$SPARK_TAR $SPARK_HDFS_LIB_DIR/tar >> $COMPILE_OUT_FILE 2>&1
        hdfs dfs -put $SPARK_MR3_LIB_DIR/$SPARK_MR3_ASSEMBLY_JAR $SPARK_HDFS_LIB_DIR >> $COMPILE_OUT_FILE 2>&1

        hdfs dfs -ls -R $SPARK_HDFS_LIB_DIR 2>&1
    fi
}

upload_hdfslib_spark_main $@
