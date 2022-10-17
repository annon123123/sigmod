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
source $TEZ_BASE_DIR/tez-setup.sh

function upload_hdfslib_tez_print_usage {
    echo "Usage: upload_hdfslib-tez.sh"
    echo " -h                                     Print the usage."
    tez_setup_print_usage_tezsrc
}

function upload_hdfslib_tez_parse_args {
    while [[ -n $1 ]]; do
        case "$1" in
            -h|--help)
                upload_hdfslib_tez_print_usage
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done
}

function upload_hdfslib_tez_main {
    tez_setup_parse_args_common $@
    upload_hdfslib_tez_parse_args $REMAINING_ARGS

    common_setup_init
    tez_setup_init

    COMPILE_OUT_FILE=${COMPILE_OUT_FILE:-$LOG_BASE_DIR/upload-hdfslib-tez.log}

    has_hdfs=$(which hdfs 2>> $COMPILE_OUT_FILE | wc -l)
    if [[ $has_hdfs -gt 0 ]]; then
        hdfs dfs -rm $TEZ_HDFS_LIB_DIR/tar/tez-$TEZ_REV.tar.gz >> $COMPILE_OUT_FILE 2>&1

        echo -e "\n# Uploading tez-$TEZ_REV jar files to hdfs #"
        echo -e "Output (HDFS): $TEZ_HDFS_LIB_DIR\n"
        hdfs dfs -mkdir -p $TEZ_HDFS_LIB_DIR/tar >> $COMPILE_OUT_FILE 2>&1

        pushd $TEZ_LIB_BASE_DIR > /dev/null
        rm -rf tez-$TEZ_REV.tar.gz
        tar -czf tez-$TEZ_REV.tar.gz -C tez-$TEZ_REV .
        popd > /dev/null

        hdfs dfs -put $TEZ_LIB_BASE_DIR/tez-$TEZ_REV.tar.gz $TEZ_HDFS_LIB_DIR/tar >> $COMPILE_OUT_FILE 2>&1

        hdfs dfs -ls -R $TEZ_HDFS_LIB_DIR 2>&1
    fi
}

upload_hdfslib_tez_main $@
