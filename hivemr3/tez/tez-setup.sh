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

function tez_setup_print_usage_tezsrc {
#   echo " --tezsrc3                              Choose tez3-mr3 (based on Tez 0.9.1) (default)."
  true 
}

function tez_setup_parse_args_common {
    LOCAL_MODE=true
    CONF_TYPE=local

    while [[ -n $1 ]]; do
        case "$1" in
            --local)
                LOCAL_MODE=true
                CONF_TYPE=local
                shift
                ;;
            --cluster)
                LOCAL_MODE=false
                CONF_TYPE=cluster
                shift
                ;;
            --tpcds)
                LOCAL_MODE=false
                CONF_TYPE=tpcds
                shift
                ;;         
            *)
                REMAINING_ARGS="$REMAINING_ARGS $1"
                shift
                ;;
        esac
    done
}

#
# for running jobs
#

function tez_setup_init {
    TEZ_SRC_REV=$(common_setup_get_git_rev $TEZ_SRC)

    TEZ_LIB_BASE_DIR=$TEZ_BASE_DIR/tezjar
    TEZ_LIB_DIR=$TEZ_LIB_BASE_DIR/tez-$TEZ_REV
    TEZ_CLASSPATH=$TEZ_LIB_DIR/*:$TEZ_LIB_DIR/lib/*

    TEZ_HDFS_LIB_DIR=$HDFS_LIB_DIR/tez
}

function tez_setup_update_conf_dir {
    conf_dir=$1
    conf_type=$2

    # HIVE-19827
    export TEZ_CONF_DIR=$BASE_DIR/conf/$conf_type/tez

    cp -r $TEZ_CONF_DIR/* $conf_dir
}

function tez_setup_check_compile {
    local_mode=$1

    is_compile_local=$(ls -q1 $TEZ_LIB_DIR | wc -l)
    if [[ $is_compile_local = "0" ]]; then
        printf "\n# Please compile tez ($TEZ_LIB_DIR is empty) #\n\n"
        exit 1
    fi

    if [[ $local_mode = false ]]; then
        is_compile_cluster=$(hdfs dfs -ls $TEZ_HDFS_LIB_DIR 2> /dev/null | wc -l)
        if [[ $is_compile_cluster = "0" ]]; then
            printf "\n# Please compile tez ($TEZ_HDFS_LIB_DIR in hdfs is empty) #\n\n"
            exit 1
        fi
    fi
}

function tez_setup_update_yarn_opts {
    local_mode=$1

    if [[ $local_mode = false ]]; then
        if [[ $LOCALIZE = false ]]; then
            TEZ_LIBURIS_OPTS="-Dliburis="
            TEZ_IGNORE_LIBURIS_OPTS="-Dignoreliburis=true"
            TEZ_AUXURIS_OPTS="-Dauxuris="
            TEZ_ADD_CLASSPATH_OPTS="-Daddclasspath=$REMOTE_BASE_DIR/tez/tez-$TEZ_REV/*:$REMOTE_BASE_DIR/tez/tez-$TEZ_REV/lib/*:$REMOTE_BASE_DIR/mr3/mr3jar/*:$REMOTE_BASE_DIR/mr3/mr3lib/*"
        else
            TEZ_LIBURIS_OPTS="-Dliburis=$TEZ_HDFS_LIB_DIR/tar/tez-$TEZ_REV.tar.gz"
            TEZ_IGNORE_LIBURIS_OPTS="-Dignoreliburis=false"
            TEZ_AUXURIS_OPTS="-Dauxuris=$TEZ_HDFS_LIB_DIR"
            TEZ_ADD_CLASSPATH_OPTS="-Daddclasspath="
        fi

        TEZ_NATIVE_LIB="-Dnative.lib=$HADOOP_NATIVE_LIB"

        export YARN_OPTS="$YARN_OPTS $TEZ_LIBURIS_OPTS $TEZ_IGNORE_LIBURIS_OPTS $TEZ_AUXURIS_OPTS $TEZ_ADD_CLASSPATH_OPTS $TEZ_NATIVE_LIB"
    fi
}

