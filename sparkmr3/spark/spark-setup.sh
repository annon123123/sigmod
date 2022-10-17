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

function spark_setup_parse_args_common {
    LOCAL_MODE=false
    CONF_TYPE=cluster
    EXTRA_DRIVER_OPTIONS=""
    EXTRA_DRIVER_CLASS_PATH=""
    EXTRA_JARS=""

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
            --amprocess)
                AM_PROCESS=true
                shift
                ;;
            --master)
                shift 2
                ;;
            --driver-java-options)
                EXTRA_DRIVER_OPTIONS=$2
                shift 2
                ;;
            --driver-class-path)
                EXTRA_DRIVER_CLASS_PATH=$2
                shift 2
                ;;
            --jars)
                EXTRA_JARS=$2
                shift 2
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

function spark_setup_init {
    SPARK_LIB_BASE_DIR=$SPARK_BASE_DIR/sparkjar
    SPARK_MR3_LIB_DIR=$SPARK_LIB_BASE_DIR/sparkmr3
    SPARK_MR3_ASSEMBLY_JAR=spark-mr3-$SPARK_MR3_REV-assembly.jar
    SPARK_HDFS_LIB_DIR=$HDFS_LIB_DIR/spark
    SPARK_TAR=spark.tar.gz
}

function spark_setup_init_conf {
    declare conf_type=$1

    # We will export SPARK_CONF_DIR after we collect all configuration files.
    SPARK_CONF_DIR=$BASE_DIR/conf/$conf_type/spark
}

function spark_setup_init_output_dir {
    declare local_mode=$1
    declare conf_type=$2
    declare output_dir=$3

    mkdir -p $output_dir > /dev/null 2>&1

    spark_setup_create_output_dir $output_dir

    common_setup_get_command > $OUT/command
    common_setup_get_git_info $MR3_SRC $OUT/mr3-dm-info
    common_setup_get_git_info $BASE_DIR $OUT/mr3-run-info
    common_setup_get_git_info $SPARK_MR3_SRC $OUT/spark-mr3-info

    OUT_CONF=$OUT/conf
    mkdir -p $OUT_CONF > /dev/null 2>&1

    cp $BASE_DIR/env.sh $OUT_CONF
    hadoop_setup_update_conf_dir $OUT_CONF $conf_type
    mr3_setup_update_conf_dir $OUT_CONF $conf_type
    spark_setup_update_conf_dir $OUT_CONF
    common_setup_update_conf_dir $OUT_CONF $conf_type
}

function spark_setup_create_output_dir {
    declare base_dir=$1

    SCRIPT_START_TIME=$(date +%s)
    declare time_stamp="$(common_setup_get_time $SCRIPT_START_TIME)"
    export OUT=$base_dir/spark-mr3-$time_stamp-$(uuidgen | awk -F- '{print $1}')
    mkdir -p $OUT > /dev/null 2>&1
    echo -e "Output Directory: \n$OUT\n"
}

function spark_setup_update_conf_dir {
    declare conf_dir=$1

    cp -r $SPARK_CONF_DIR/* $conf_dir
    rm -f $conf_dir/*.template

    export SPARK_CONF_DIR=$conf_dir
}

function spark_setup_config_spark_logs {
    declare output_dir=$1

    mkdir -p $output_dir
}

function spark_setup_init_run_configs {
    declare out_dir=$1
    declare local_mode=$2
    declare am_process=$3

    if [[ $local_mode = true ]]; then
        if [[ $am_process = true ]]; then
            am_mode="local-process"
        else
            am_mode="local-thread"
            export HADOOP_OPTS="$HADOOP_OPTS -Dmr3.root.logger=INFO,file,console"
        fi
    else
        if [[ $am_process = true ]]; then
            am_mode="local-process"
        else
            am_mode="yarn"
        fi
    fi

    mr3_setup_update_yarn_opts $local_mode $log_dir $am_mode true

    export HADOOP_OPTS="$HADOOP_OPTS $YARN_OPTS"

    # MR3 configurations specific to Spark runtime
    export HADOOP_OPTS="$HADOOP_OPTS -Dmr3.runtime=spark -Dmr3.async.logging=false -Dmr3.container.close.filesystem.ugi=false"
}

# SPARK_DRIVER_CP = jars only for Driver
# SPARK_DRIVER_JARS = jars for both Driver and Executors; automatically downloaded by Executors

function spark_setup_init_driver_opts {
    SPARK_DRIVER_OPTS="$HADOOP_OPTS"
    SPARK_DRIVER_CP="$MR3_LIB_DIR/$MR3_SPARK_ASSEMBLY_JAR:$SPARK_MR3_LIB_DIR/$SPARK_MR3_ASSEMBLY_JAR"
}

function spark_setup_extra_opts {
    SPARK_DRIVER_OPTS="$EXTRA_DRIVER_OPTIONS $SPARK_DRIVER_OPTS"
    SPARK_DRIVER_CP="$EXTRA_DRIVER_CLASS_PATH:$MR3_CONF_DIR:$SPARK_DRIVER_CP"
    SPARK_DRIVER_JARS="$EXTRA_JARS"
}
