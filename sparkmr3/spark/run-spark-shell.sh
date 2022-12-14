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
source $HADOOP_BASE_DIR/hadoop-setup.sh
source $MR3_BASE_DIR/mr3-setup.sh
source $SPARK_BASE_DIR/spark-setup.sh

function run_spark_shell_init {
    common_setup_init
    hadoop_setup_init
    mr3_setup_init
    spark_setup_init
    spark_setup_init_conf $CONF_TYPE

    BASE_OUT=$SPARK_BASE_DIR/spark-shell-result
    spark_setup_init_output_dir $LOCAL_MODE $CONF_TYPE $BASE_OUT
}

function run_spark_shell_main {
    spark_setup_parse_args_common $@

    run_spark_shell_init

    declare log_dir="$OUT/spark-logs"
    declare out_file="$log_dir/out-spark-shell.txt"

    spark_setup_config_spark_logs $log_dir
    spark_setup_init_run_configs $OUT $LOCAL_MODE $AM_PROCESS
    spark_setup_init_driver_opts
    spark_setup_extra_opts

    # use '--master spark' because '--master mr3' is rejected from Spark 3.0.3
    $SPARK_HOME/bin/spark-shell \
        --driver-java-options "$SPARK_DRIVER_OPTS" \
        --master spark \
        --jars "$SPARK_DRIVER_JARS" \
        --driver-class-path "$SPARK_DRIVER_CP" \
        --conf spark.hadoop.yarn.timeline-service.enabled=false \
        $REMAINING_ARGS \
        2>&1
    # do not redirect with '| tee $out_file' because CTRL-C causes Spark shell to stop responding
}

run_spark_shell_main $@
