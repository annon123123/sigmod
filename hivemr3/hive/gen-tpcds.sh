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
source $TEZ_BASE_DIR/tez-setup.sh
source $MR3_BASE_DIR/mr3-setup.sh
source $HIVE_BASE_DIR/hive-setup.sh

function print_usage {
    echo "Usage: gen-tpcds.sh [option]"
    common_setup_print_usage_common_options
    common_setup_print_usage_conf_mode
    hive_setup_print_usage_hivesrc
    tez_setup_print_usage_tezsrc
    hadoop_setup_print_usage_get_yarn_logs
    echo ""
    echo "Generate data based on HIVE_DS_FORMAT and HIVE_DS_SCALE_FACTOR defined in env.sh."
    echo "Data is generated in the following directory if it doesn't exist:"
    echo "  /tmp/tpcds-generate/[HIVE_DS_SCALE_FACTOR]"
    echo "A MapReduce job creates temporary files, which may be deleted if the process is interrupted, such as:"
    echo "  /tmp/all_2-*"
    echo "  /tmp/-2be2732670ff4dfb167ff6671307bd82.jar"
    echo ""
}

function warning {
    common_setup_log_warning ${FUNCNAME[1]} "$1"
}

function error {
    common_setup_log_error ${FUNCNAME[1]} "$1"
    print_usage
    exit 1
}

function parse_args {
    if [ $# = 0 ]; then
       print_usage
       exit 1 
    fi

    hive_setup_parse_args_common $@

    MAX_GET_YARN_REPORT_TRIES=0

    while [ "${1+defined}" ]; do
      case "$1" in
        -h|--help)
          print_usage
          exit 1
          ;;
        --get-yarn-logs)
          MAX_GET_YARN_REPORT_TRIES=10
          shift
          ;;
        *)
          shift
          ;;
      esac
    done

    if [[ ! -z $HIVE_SERVER2_PORT_NEW ]]; then
        HIVE_SERVER2_PORT=$HIVE_SERVER2_PORT_NEW
    fi
    if [[ ! -z $HIVE_SERVER2_KERBEROS_PRINCIPAL_NEW ]]; then
        HIVE_SERVER2_KERBEROS_PRINCIPAL=$HIVE_SERVER2_KERBEROS_PRINCIPAL_NEW
    fi
}

function run_hive_ds_init {
    if [ $LOCAL_MODE = true ]; then
        export HADOOP_HOME=$HADOOP_HOME_LOCAL
    fi

    # setup environment variables, e.g. HIVE_HOME, PATH
    common_setup_init
    hadoop_setup_init
    tez_setup_init
    mr3_setup_init
    hive_setup_init
    hive_setup_check_hive_home || error "HIVE_HOME not a directory: $HIVE_HOME"
    hive_setup_init_conf $CONF_TYPE

    if [ $LOCAL_MODE = true ]; then
        export HIVE_METASTORE_PORT=$HIVE_METASTORE_LOCAL_PORT
    fi

    hive_setup_init_heapsize_mb $HIVE_CLIENT_HEAPSIZE

    tpc_ds_init

    if [ -z "$FORMAT" ]; then
        set_file_format "${HIVE_DS_FORMAT:-orc}"
    fi

    BASE_OUT=$HIVE_BASE_DIR/gen-tpcds-result
    RUN_TIMES_HEADER="filename,status,time,rows"

    echo -e "\n# Generating data using Hive-MR3 ($HIVE_REV, $TEZ_REV) #\n" >&2
    # setup output directory, e.g. conf/ stores all configuration files
    hive_setup_init_output_dir $LOCAL_MODE $CONF_TYPE $BASE_OUT
}

function tpc_ds_init {
    if [ -z "$HIVE_DS_SCALE_FACTOR" ]; then
        error "HIVE_DS_SCALE_FACTOR not set in env.sh"
    fi

    HIVE_BENCHMARKS=$HIVE_BASE_DIR/benchmarks
    TPC_DS_DIR=$HIVE_BENCHMARKS/hive-testbench
    TPC_DS_BUILD=$TPC_DS_DIR/tpcds-build.sh
    TPC_DS_DATA_GEN=$TPC_DS_DIR/tpcds-setup.sh
}

function set_file_format {
    declare -l file_format=$1

    case "$file_format" in
      orc|rcfile|textfile)
        export FORMAT="$file_format"
        ;;
      *)
        error "Unsupported file format: $file_format"
        ;;
    esac
}

function log_runtimes {
    declare runtimes_file=$1
    declare run_id=$2
    declare run_status_code=$3
    declare run_time=$4
    declare rows_returned=${5:-0}

    declare run_status="failed"
    if [ $run_status_code = 0 ]; then
        run_status="success"
    fi

    declare runtime_entry_row="$run_id,$run_status,$run_time,$rows_returned"
    echo $runtime_entry_row >> $runtimes_file
}

function generate_data {
    declare -i return_code=0

    declare CURRENT_RUN_DIR="$OUT/run-gen"
    mkdir -p $CURRENT_RUN_DIR

    declare gen_times_file="$CURRENT_RUN_DIR/gen-times.csv"
    declare gen_hivelogs_dir="$CURRENT_RUN_DIR/hive-logs"
    declare gen_output_file="$CURRENT_RUN_DIR/out-gen.txt"

    # run files that are built are looked for in cwd
    pushd $TPC_DS_DIR > /dev/null

    ls -q1 | grep -vE 'ddl-tpcds|ddl-tpch|README.md|runSuite.pl|sample-queries-tpcds-hive1|sample-queries-tpcds-hive2|sample-queries-tpcds-hive4|sample-queries-tpch|settings|tpcds-build.sh|tpcds-gen|tpcds-setup.sh|tpch-build.sh|tpch-gen|tpch-setup.sh' | xargs rm -rf

    pushd tpcds-gen > /dev/null
    ls -q1 | grep -vE 'Makefile|patches|pom.xml|README|README|README.md|src|tpcds_kit.zip' | xargs rm -rf
    popd > /dev/null

    $TPC_DS_BUILD >> $gen_output_file 2>&1

    echo "Generating TPC-DS Data with scale factor of $HIVE_DS_SCALE_FACTOR GB..." >> $gen_output_file
    echo "$RUN_TIMES_HEADER" > $gen_times_file

    echo "TPC-DS Data Generation running with MR3..." >> $gen_output_file
    hive_setup_set_hive_opts "--hiveconf hive.execution.engine=mr3 --hiveconf hive.execution.mode=container"

    hive_setup_config_hive_logs $gen_hivelogs_dir
    declare cmd="$TPC_DS_DATA_GEN $HIVE_DS_SCALE_FACTOR"

    # confine hive_setup_exec_cmd_timed's global vars to this functions scope
    declare EXEC_EXIT_CODE EXEC_TIMED_RUN_START EXEC_TIMED_RUN_STOP EXEC_TIMED_RUN_TIME
    hive_setup_exec_cmd_timed "$cmd" "$gen_output_file" true
    return_code=$EXEC_EXIT_CODE

    # check for errors in generation output
    if grep -qE "^make:.*Error|java.io.IOException|FAILED" $gen_output_file; then
        grep -E "^make:.*Error|java.io.IOException|FAILED" $gen_output_file
        return_code=1
    fi

    declare gen_id="data_generation_"$HIVE_DS_SCALE_FACTOR"GB"
    log_runtimes $gen_times_file "$gen_id" $return_code "$EXEC_TIMED_RUN_TIME"

    declare run_num=1
    hive_setup_report_run_results "$gen_id" \
        $run_num \
        $return_code \
        $return_code \
        $EXEC_TIMED_RUN_START \
        $EXEC_TIMED_RUN_TIME \
        $CURRENT_RUN_DIR \
        "$CURRENT_RUN_DIR/exceptions-gen.txt"

    popd > /dev/null

    return $return_code
}

function main {
    declare -i return_code=0

    parse_args $@
    run_hive_ds_init

    pushd $BASE_DIR > /dev/null
    # update HADOOP_OPTS and HADOOP_CLASSPATH
    hive_setup_init_run_configs $OUT $LOCAL_MODE false $HIVE_METASTORE_PORT $LOG_LEVEL

    common_setup_output_header

    generate_data
    return_code=$?

    common_setup_avg_time $OUT/run-results
    if [[ $NUM_RUNS -gt 1 ]]; then
        tail -n1 $OUT/run-results
    fi

    popd > /dev/null #Return to original directory

    exit $return_code
}

main $@
