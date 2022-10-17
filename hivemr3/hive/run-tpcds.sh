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

function run_hive_ds_print_usage {
    echo "Usage: run-tpcds.sh [option(s)]"
    common_setup_print_usage_common_options
    common_setup_print_usage_conf_mode
    hive_setup_print_usage_hivesrc
    tez_setup_print_usage_tezsrc
    echo " -q <num>                               Run a single TPC-DS query where num=[1..99]."
    echo " -n <num>                               Specify the number of times that the query runs (default = 1)."
    echo " --beeline                              Execute queries using Beeline."
    echo " --querystart <num>                     Specify the starting TPC-DS query."
    echo " --queryend <num>                       Specify the last TPC-DS query."
    echo "                                          Ex. If there are TPC-DS queries 3 to 98:"
    echo "                                            --querystart 17: run queries 17 to 98."
    echo "                                            --queryend 22: run queries 3 to 22."
    echo "                                            --querystart 40 --queryend 55: run queries 40 to 55."
    echo "                                            --querystart40 --queryend39: run queries 40 to 98, and then queries 3 to 39."
    echo " --queryskip <num>                      Skip a query."
    echo " --session                              Execute all queries in the same session"
    echo "   --repeat <num>                       Repeat the same sequence of queries."
    echo " --timeout <time in s/m/h>              Timeout for running a session, e.g. 60s, 10m, 2h"
    mr3_setup_print_usage_amprocess
    echo " --hiveserver2-host <host>              Update HiveServer2 host (overridng HIVE_SERVER2_HOST in env.sh)."
    echo " --hiveserver2-port <port>              Update HiveServer2 port (overridng HIVE_SERVER2_PORT in env.sh)."
    echo " --hiveserver2-principal <principal>    Update HiveServer2 principal (overriding HIVE_SERVER2_KERBEROS_PRINCIPAL in env.sh)."
    echo " --hiveserver2-jdbc-opts <opts>         Update HiveServer2 JDBC options."
    echo " --spark                                Run on Spark"
    echo " --presto                               Run on Presto"
    echo " --test                                 Check logs for exceptions."
    echo " --kill <time in s>                     Kill the first query in <time> seconds (with Timeline Server running)"
    hadoop_setup_print_usage_get_yarn_logs
    hive_setup_print_usage_hiveconf
    echo ""
    echo "Examples:"
    echo " run-tpcds.sh --local -q 12"
    echo " run-tpcds.sh --tpcds --beeline --session --querystart 12 --queryend 18"
    echo "" 
}

function warning {
    common_setup_log_warning ${FUNCNAME[1]} "$1"
}

function error {
    common_setup_log_error ${FUNCNAME[1]} "$1"
    run_hive_ds_print_usage
    exit 1
}

function parse_args {
    if [ $# = 0 ]; then
       run_hive_ds_print_usage
       exit 1 
    fi

    RUN_QUERY="all"
    BEELINE=false
    SESSION=false
    AM_PROCESS=false
    TEST_MODE=false
    DELETE_MODE=false
    NUM_RUNS=1
    QUERY_SKIP=""
    TIMEOUT=""
    NUM_REPEAT=1
    MAX_GET_YARN_REPORT_TRIES=0
    KILL_TIME_SEC=0
    KILL_K8S_ALL_TIME_SEC=0
    KILL_K8S_AM_TIME_SEC=0
    BEELINE_IDX=1

    while [ "${1+defined}" ]; do
      case "$1" in
        -h|--help)
          run_hive_ds_print_usage
          exit 1
          ;;
        -q)
          if ! [ "$2" ]; then
             error "-q option missing argument: query number"
          fi
          common_setup_check_integer $2 || error '-q invalid argument: must be a number'
          RUN_QUERY=$2
          shift 2
          ;;
        -n)
          if ! [ "$2" ]; then
             error "-n option missing argument: number of runs"
          fi
          common_setup_check_integer $2 || error '-n invalid argument: must be a number'
          NUM_RUNS=$2
          if [ $NUM_RUNS -lt 1 ]; then
              error '-n invalid argument: must be larger than 0'
          fi
          shift 2
          ;;
        --session)
          SESSION=true
          shift
          ;;
        --beeline)
          BEELINE=true
          shift
          ;;
        --hiveserver2-host)
          HIVE_SERVER2_HOST_CUSTOM=$2
          shift 2
          ;;
        --hiveserver2-port)
          HIVE_SERVER2_PORT_CUSTOM=$2
          shift 2
          ;;
        --hiveserver2-principal)
          HIVE_SERVER2_KERBEROS_PRINCIPAL_CUSTOM=$2
          shift 2
          ;;
        --hiveserver2-jdbc-opts)
          HIVE_SERVER2_JDBC_OPTS=$2
          shift 2
          ;;
        --amprocess)
          AM_PROCESS=true
          shift
          ;;
        --spark|--spark3|--sparkmr3)
          USE_SPARK=true
          HIVE_SERVER2_HOST_CUSTOM=$SPARK_THRIFT_SERVER_HOST
          HIVE_SERVER2_PORT_CUSTOM=$SPARK_THRIFT_SERVER_PORT
          HIVE_SERVER2_KERBEROS_PRINCIPAL_CUSTOM=$SPARK_THRIFT_SERVER_PRINCIPAL
          BEELINE_PATH_CUSTOM=$SPARK_BEELINE_PATH
          BEELINE=true
          shift
          ;;
        --spark2)
          USE_SPARK=true
          HIVE_SERVER2_HOST_CUSTOM=$SPARK_THRIFT_SERVER_HOST
          HIVE_SERVER2_PORT_CUSTOM=$SPARK_THRIFT_SERVER_PORT
          HIVE_SERVER2_KERBEROS_PRINCIPAL_CUSTOM=$SPARK_THRIFT_SERVER_PRINCIPAL
          BEELINE_PATH_CUSTOM=$SPARK2_SRC/bin/beeline
          BEELINE=true
          shift
          ;;
        --presto)
          USE_PRESTO=true
          BEELINE=true
          shift
          ;;
        --impala)
          USE_IMPALA=true
          BEELINE=true
          shift
          ;;
        --querystart)
          QUERY_START=$2
          shift 2
          ;;
        --queryend)
          QUERY_END=$2
          shift 2
          ;;
        --queryskip)
          QUERY_SKIP="$QUERY_SKIP $2"
          shift 2
          ;;
        --repeat)
          NUM_REPEAT=$2
          shift 2
          ;;
        --timeout)
          TIMEOUT="timeout $2"
          shift 2
          ;;
        --test)
          TEST_MODE=true
          shift
          ;;
        --kill)
          KILL_TIME_SEC=$2
          shift 2
          ;;
        --kill-k8s-all)
          KILL_K8S_ALL_TIME_SEC=$2
          shift 2
          ;;
        --kill-k8s-am)
          KILL_K8S_AM_TIME_SEC=$2
          shift 2
          ;;
        --get-yarn-logs)
          MAX_GET_YARN_REPORT_TRIES=10
          shift
          ;;
        --delete)
          DELETE_MODE=true
          shift
          ;;
        --beeline-idx)
          BEELINE_IDX=$2
          shift 2
          ;;
        *)
          export HIVE_OPTS="$HIVE_OPTS $@"
          break
          ;;
      esac
    done

    if [[ $NUM_REPEAT -gt 1 ]] && [[ $SESSION = false ]]; then
        echo "Wrong use of '--repeat $NUM_REPEAT' because it is valid only if '--session' is provided"
        exit 1
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

    # comment out for running Impala
    hive_setup_check_hive_home || error "HIVE_HOME not a directory: $HIVE_HOME"

    hive_setup_init_conf $CONF_TYPE

    if [ $LOCAL_MODE = true ]; then
        export HIVE_METASTORE_PORT=$HIVE_METASTORE_LOCAL_PORT
    fi

    hive_setup_init_heapsize_mb $HIVE_CLIENT_HEAPSIZE

    # setup environment variables for the tpcds benchmark from hive-testbench
    tpc_ds_init

    if [ -z "$FORMAT" ]; then
        set_file_format "${HIVE_DS_FORMAT:-orc}"
    fi

    BASE_OUT=$HIVE_BASE_DIR/run-tpcds-result
    RUN_TIMES_HEADER="filename,status,time,rows"

    hive_setup_init_output_dir $LOCAL_MODE $CONF_TYPE $BASE_OUT
}

function tpc_ds_init {
    if [ -z "$HIVE_DS_SCALE_FACTOR" ]; then
        error "HIVE_DS_SCALE_FACTOR not set in env.sh"
    fi

    HIVE_BENCHMARKS=$HIVE_BASE_DIR/benchmarks
    TPC_DS_DIR=$HIVE_BENCHMARKS/hive-testbench
    TPC_DS_RUN_ALL=$TPC_DS_DIR/runSuite.pl
    if [[ $HIVE_SRC_TYPE = 2 ]]; then
        TPC_DS_QUERY_DIR=$TPC_DS_DIR/sample-queries-tpcds-hive2
    fi
    if [[ $HIVE_SRC_TYPE = 3 ]]; then
        TPC_DS_QUERY_DIR=$TPC_DS_DIR/sample-queries-tpcds-hive2
    fi
    if [[ $HIVE_SRC_TYPE = 4 ]]; then
        TPC_DS_QUERY_DIR=$TPC_DS_DIR/sample-queries-tpcds-hive2
    fi
    TPS_DS_SETTINGS=$TPC_DS_QUERY_DIR/testbench.settings

    if [[ $USE_SPARK = true ]]; then
        TPC_DS_DIR=$HIVE_BENCHMARKS/sparksql
        TPC_DS_QUERY_DIR=$HIVE_BENCHMARKS/sparksql
    fi

    if [[ $USE_PRESTO = true ]]; then
        #TPC_DS_DIR=$HIVE_BENCHMARKS/presto
        #TPC_DS_QUERY_DIR=$HIVE_BENCHMARKS/presto
        TPC_DS_DIR=$HIVE_BENCHMARKS/hive-testbench/sample-queries-tpcds-hive4-presto/
        TPC_DS_QUERY_DIR=$HIVE_BENCHMARKS/hive-testbench/sample-queries-tpcds-hive4-presto/
    fi

    if [[ $USE_IMPALA = true ]]; then
        TPC_DS_DIR=$HIVE_BENCHMARKS/hive-testbench/sample-queries-tpcds-hive4
        TPC_DS_QUERY_DIR=$HIVE_BENCHMARKS/hive-testbench/sample-queries-tpcds-hive4
    fi
}

function set_file_format {
    declare -l file_format=$1

    case "$file_format" in
      orc|rcfile|textfile|parquet)
        export FORMAT="$file_format"
        ;;
      *)
        error "Unsupported file format: $file_format"
        ;;
    esac
}

function check_query_logs {
    declare runtimes_file=$1
    declare query_filename=$2
    declare query_name=$3
    declare yarn_log_dir=$4
    declare ex_file=$5

    declare hive_log_file_pattern="$CURRENT_HIVE_LOG_DIR/*$query_name*"

    declare -i return_code=0

    grep -i "$query_filename,failed" $runtimes_file >> $ex_file
    [ $? = 0 ] && let return_code=1

    hive_setup_check_exceptions "$hive_log_file_pattern" $ex_file
    [ $? != 0 ] && let return_code=1

    if [ -d "$yarn_log_dir" ]; then
        hive_setup_check_exceptions "$yarn_log_dir/*" $ex_file
        [ $? != 0 ] && let return_code=1
    fi

    return $return_code
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

# get the running time
function process_query_results {
    declare cmd_id=$1
    declare -i return_code=$2
    declare query_output_file=$3
    declare runtimes_file=$4

    if egrep "^FAILED: " $query_output_file > /dev/null; then
        return_code=1
    fi

    declare rows
    if [ $return_code = 0 ]; then
        declare result="$(grep "Time taken:.*seconds, Fetched:.*row(s)" $query_output_file)"
        if [ -n "$result" ]; then
            rows=$([[ $result =~ Fetched:[[:space:]]([0-9]+) ]] && echo ${BASH_REMATCH[1]});
        fi
    fi

    log_runtimes $runtimes_file "$cmd_id" $return_code "$EXEC_TIMED_RUN_TIME" "${rows-0}"

    if [[ $USE_PRESTO = true ]]; then
        echo "Query running time: $EXEC_TIMED_RUN_TIME" | tee -a $runtimes_file
    elif [[ $BEELINE = false ]]; then
        (grep "Time taken:" $query_output_file || echo "(FAILED:$EXEC_TIMED_RUN_TIME )") | while read line; do echo "$line"; done | tee -a $runtimes_file
        declare numlines="$(grep "Time taken:" $query_output_file | wc | awk '{print $1}')"
        echo "Number of lines = $numlines"
        declare numfetched="$(grep "Time taken:" $query_output_file | grep "Fetched:" | wc | awk '{print $1}')"
        echo "Number of Fetched = $numfetched"
    else
        (grep "selected" $query_output_file || echo "(FAILED:$EXEC_TIMED_RUN_TIME )") | while read line; do echo "Query running time: $line"; done | tee -a $runtimes_file
    fi

    grep -B2 "1 row selected" $query_output_file | tee -a $runtimes_file

    #cat $runtimes_file

    return $return_code
}

function get_query_name_from_filename {
    declare input_string=$1

    if [[ $input_string =~ .*/((query([0-9]+)).sql)$ ]]; then
        QUERY_FILENAME=${BASH_REMATCH[1]}
        QUERY_NAME=${BASH_REMATCH[2]}
    elif [[ $input_string =~ .*/((all_tpcds_queries).sql)$ ]]; then
        QUERY_FILENAME=${BASH_REMATCH[1]}
        QUERY_NAME=${BASH_REMATCH[2]}
    #elif [[ $query_file =~ .*/([^/]*)$ ]]; then
    #    QUERY_FILENAME=${BASH_REMATCH[1]}
    #    QUERY_NAME=${BASH_REMATCH[1]}
    else
        return 1
    fi

    return 0
}

function run_query {
    declare query_file=$1
    if [ ! -f "$query_file" ]; then
        error "$query_file file does not exist."
    fi

    declare -i return_code=0

    declare QUERY_FILENAME
    declare QUERY_NAME

    get_query_name_from_filename $query_file
    if [ $? != 0 ]; then
        warning "Unable to determine query name from file: $query_file"
        return 1
    fi

    HIVE_QUERY_LOG_NAME="hive-$QUERY_NAME.log"
    declare hive_output_log_file="$CURRENT_HIVE_LOG_DIR/out-$QUERY_NAME.txt"
    declare hive_tpcds_database="tpcds_bin_partitioned_"$FORMAT"_$HIVE_DS_SCALE_FACTOR"
    declare hive_runtime_file=$CURRENT_RUN_DIR/$CURRENT_RUNTIME_FILENAME
    declare error_file="$CURRENT_RUN_DIR/exceptions-${QUERY_NAME}.txt"
    declare yarn_log_dir="$CURRENT_RUN_DIR/yarn-logs/$QUERY_NAME"

    # submit query via HiveCLI or Beeline
    echo "Running Hive with $QUERY_NAME ($query_file)..." >> $hive_output_log_file

    if [[ $USE_PRESTO = true ]]; then
        declare cmd="/home/gitlab-runner/mr3-run/presto/run_query.sh $query_file"
    elif [[ $USE_IMPALA = true ]]; then
        declare cmd="impala-shell --impalad=blue1 --database=tpcds_${HIVE_DS_SCALE_FACTOR}_${FORMAT} --query_file=$query_file"
    elif [[ $BEELINE = true ]]; then
        hiveserver2_host=${HIVE_SERVER2_HOST_CUSTOM:-$HIVE_SERVER2_HOST}
        hiveserver2_port=${HIVE_SERVER2_PORT_CUSTOM:-$HIVE_SERVER2_PORT}
        if [[ $HIVE_SERVER2_AUTHENTICATION = KERBEROS ]]; then
            principal_name="principal=${HIVE_SERVER2_KERBEROS_PRINCIPAL_CUSTOM:-$HIVE_SERVER2_KERBEROS_PRINCIPAL}"
        else
            principal_name=""
        fi
        jdbc_options=$HIVE_SERVER2_JDBC_OPTS

        beeline_path=${BEELINE_PATH_CUSTOM:-$(which beeline)}

        # for the following error, export USE_HDP=true:
        #   java.lang.IllegalArgumentException: Cannot modify hive.querylog.location at runtime.
        if [[ $USE_SPARK = true ]] || [[ $USE_HDP = true ]] || [[ $USE_K8S = true ]] ; then
          if [[ $USE_SPARK = true ]]; then
            hive_tpcds_database="tpcds_parquet_$HIVE_DS_SCALE_FACTOR"
          fi
          echo $HIVE_OPTS
          declare cmd="$TIMEOUT $beeline_path -u \"jdbc:hive2://$hiveserver2_host:$hiveserver2_port/${hive_tpcds_database};${principal_name};${jdbc_options}\" -n $USER -p $USER \
-i $TPS_DS_SETTINGS \
-f $query_file"
        else  # Hive-MR3 Beeline
          declare cmd="$TIMEOUT $beeline_path -u \"jdbc:hive2://$hiveserver2_host:$hiveserver2_port/${hive_tpcds_database};${principal_name};${jdbc_options}\" -n $USER -p $USER \
-i $TPS_DS_SETTINGS \
--hiveconf hive.querylog.location=$CURRENT_HIVE_LOG_DIR \
--hiveconf mr3.am.local.log-dir=$CURRENT_RUN_DIR \
-f $query_file"
        fi
    else  # Hive-MR3 CLI 
        declare cmd="$TIMEOUT hive -i $TPS_DS_SETTINGS \
--hiveconf hive.log.file=$HIVE_QUERY_LOG_NAME \
--hiveconf hive.log.dir=$CURRENT_HIVE_LOG_DIR \
--hiveconf hive.querylog.location=$CURRENT_HIVE_LOG_DIR \
--hiveconf mr3.am.local.log-dir=$CURRENT_RUN_DIR \
-e 'use $hive_tpcds_database; source $query_file;'"
    fi

    export HADOOP_OPTS="$HADOOP_OPTS -Djavax.security.auth.useSubjectCredsOnly=false"

    # confine hive_setup_exec_cmd_timed's global vars to this functions scope
    declare EXEC_EXIT_CODE EXEC_TIMED_RUN_START EXEC_TIMED_RUN_STOP EXEC_TIMED_RUN_TIME
    hive_setup_exec_cmd_timed "$cmd" $hive_output_log_file true
    return_code=$EXEC_EXIT_CODE

    # get the running time
    process_query_results "$QUERY_FILENAME" $return_code $hive_output_log_file $hive_runtime_file
    if [ $? -ne 0 ]; then
        return_code=1
    fi

    declare run_time_query=0

    # get yarn logs
    if [[ $LOCAL_MODE = false ]]; then
        if [[ $BEELINE = false ]]; then
            declare app_status_file="$CURRENT_RUN_DIR/application-status-${QUERY_NAME}.txt"

            # not used because currently APPID_SEARCH_REGEX is not printed in the log
            #hive_setup_get_yarn_report_from_file $hive_output_log_file \
            #                                     $app_status_file \
            #                                     $yarn_log_dir >> $hive_output_log_file 2>&1

            run_time_query=$(grep "RUNNING_TIME" $app_status_file | tail -n1 | awk '{print $2}')

            hadoop_setup_check_yarn_app_state $hive_output_log_file
            if [ $? != 0 ]; then
                return_code=1
            fi
        else
            if [[ $SESSION = true ]]; then
                runtime_file_check=$hive_runtime_file
            else
                runtime_file_check=$hive_output_log_file
            fi
            run_time_query=$(grep "selected" $runtime_file_check | cut -d'(' -f2 | cut -d' ' -f1 | awk '{sum+=$1} END {print sum}')
        fi
    fi

    declare run_time_report="$run_time_query ($EXEC_TIMED_RUN_TIME)"

    declare -i error_status_code=$return_code
    if [ $TEST_MODE = true ]; then
        check_query_logs $hive_runtime_file \
            "$QUERY_FILENAME" \
            "$QUERY_NAME" \
            $yarn_log_dir \
            $error_file

        if [ $? -ne 0 ]; then
            error_status_code=1
        fi
    fi

    run_name="$QUERY_NAME"
    if [[ $QUERY_NAME = "all_tpcds_queries" ]]; then
        run_name="$run_name-query$QUERY_START-query$QUERY_END"
    fi

    hive_setup_report_run_results "$run_name" \
        "$NUM_RUN" \
        $return_code \
        $error_status_code \
        $EXEC_TIMED_RUN_START \
        "$run_time_report" \
        "$CURRENT_RUN_DIR" \
        $error_file

    return_code=$error_status_code

    if [ $return_code = 0 ]; then
        if [ $DELETE_MODE = true ]; then
            #Delete query logs
            rm -rf $yarn_log_dir $hive_output_log_file "$CURRENT_HIVE_LOG_DIR/$HIVE_QUERY_LOG_NAME"
        fi
    fi

    return $return_code
}

function kill_first_query {
  waittime=$1
  echo "sleep $waittime seconds"
  sleep $waittime
  app_id=$(yarn application -list 2> /dev/null | tail -n 1 | awk '{print $1}') 1>/dev/null 2>/dev/null
  appattempt=$(yarn applicationattempt -list $app_id 2> /dev/null | tail -n 1 | awk '{print $1}') 1>/dev/null 2>/dev/null
  # kill DAGAppMaster
  #  - do not use -amprocess to kill DAGAppMaster
  #containerids=$(yarn container -list $appattempt | awk '{print $1}' | grep _000001) 1>/dev/null 2>/dev/null
  # kill ContainerWorkers
  #  - set tez.runtime.pipelined-shuffle.enabled to false (otherwise queries usually fail)
  containerids=$(yarn container -list $appattempt 2> /dev/null | tail -n+5 | head -n5 | awk '{print $1}') 1>/dev/null 2>/dev/null
  for containerid in $containerids; do
    echo "killing container $app_id $appattempt $containerid"
    yarn container -signal $containerid FORCEFUL_SHUTDOWN 1>/dev/null 2>/dev/null
  done
}

function run_all_queries {
    declare -i return_code=0

    # print all queryX.sql file names to 'all_tpcds_queries.tmp'
    declare session_query_tmp=$CURRENT_RUN_DIR/all_tpcds_queries.tmp
    pushd $TPC_DS_QUERY_DIR > /dev/null
    ls -q1 *.sql | awk -Fquery '{print $2}' | sort -h | while read line; do
        echo query$line >> $session_query_tmp
    done
    popd > /dev/null

    # get the index of QUERY_START and QUERY_END, e.g. from query3 to query12
    first_query=$(head -n1 $session_query_tmp)
    last_query=$(tail -n1 $session_query_tmp)

    if [[ -z $QUERY_START ]]; then
        QUERY_START=$(echo $first_query | awk -Fquery '{print $2}' | awk -F. '{print $1}')
    fi
    if [[ -z $QUERY_END ]]; then
        QUERY_END=$(echo $last_query | awk -Fquery '{print $2}' | awk -F. '{print $1}')
    fi

    if ! grep -q "query${QUERY_START}.sql" $session_query_tmp; then
        echo "query${QUERY_START}.sql does not exist"
        exit 1
    fi
    if ! grep -q "query${QUERY_END}.sql" $session_query_tmp; then
        echo "query${QUERY_END}.sql does not exist"
        exit 1
    fi

    # print all queryX.sql contents to 'all_tpcds_queries.base', which is a list of SQL commands
    declare session_query_base=$CURRENT_RUN_DIR/all_tpcds_queries.base
    if [[ $QUERY_START -lt $QUERY_END ]]; then
        sed -n "/query${QUERY_START}.sql/,/query${QUERY_END}.sql/p" $session_query_tmp >> $session_query_base
    elif [[ $QUERY_START = $QUERY_END ]]; then
        echo "query${QUERY_START}.sql" >> $session_query_base
    else
        sed -n "/query${QUERY_START}.sql/,/$last_query/p" $session_query_tmp >> $session_query_base
        sed -n "/$first_query/,/query${QUERY_END}.sql/p" $session_query_tmp >> $session_query_base
    fi

    while read num_skip; do
        sed -i "/query${num_skip}.sql/d" $session_query_base
    done < <(echo $QUERY_SKIP | tr " " "\n")

    # rearrange queryX.sql file names
    declare session_query_base_rearrange=$CURRENT_RUN_DIR/all_tpcds_queries.base.tmp
    declare -i query_num=`wc -l $session_query_base | awk '{print $1}'`
    awk -v beeline_idx=$BEELINE_IDX -v query_num=$query_num \
    'BEGIN { beeline_idx = beeline_idx - 1; cnt = 0; }
    { query[(query_num * beeline_idx + cnt - beeline_idx)%query_num] = $1; cnt++; }
    END { for (i = 0; i < cnt; i++) { print query[i]} }' $session_query_base > $session_query_base_rearrange
    mv $session_query_base_rearrange $session_query_base

    # print the queryX.sql file names from QUERY_START to QUERY_END
    declare session_query_order=$CURRENT_RUN_DIR/all_tpcds_queries.order
    for repeat_idx in $(eval echo "{1..$NUM_REPEAT}"); do
        cat $session_query_base >> $session_query_order
    done

    rm -f $session_query_tmp $session_query_base

    # if session is true, then submit all queries in one session (run 'hive' command only once)
    # otherwise, submit each query at a time (run a new 'hive' for each query)
    if [ $SESSION = true ]; then
        # cat all queries into one sql file and execute that file

        declare session_query=$CURRENT_RUN_DIR/all_tpcds_queries.sql
        #awk 'FNR==1 && NR!=1 {print ";"}{print}' $TPC_DS_QUERY_DIR/*.sql > $session_query
        cat $session_query_order | while read line; do
            cat $TPC_DS_QUERY_DIR/$line >> $session_query
            echo -e "; \n" >> $session_query
        done

        run_query $session_query
        if [ $? -ne 0 ]; then
            return_code=1
        fi
    else
        first_file=$(head -n 1 $session_query_order)
        cat $session_query_order | while read line; do
            query=$TPC_DS_QUERY_DIR/$line

            if [[ $first_file = $line ]]; then
                if [[ $KILL_K8S_ALL_TIME_SEC -gt 0 ]]; then
                    kill_k8s_all_workers $KILL_K8S_ALL_TIME_SEC &
                fi
                if [[ $KILL_TIME_SEC -gt 0 ]]; then
                    if [[ $USE_K8S = true ]]; then
                        kill_k8s_workers $KILL_TIME_SEC &
                    else
                        kill_first_query $KILL_TIME_SEC &
                    fi
                elif [[ $KILL_K8S_AM_TIME_SEC -gt 0 ]]; then
                    kill_k8s_am $KILL_K8S_AM_TIME_SEC &
                fi
            fi

            run_query $query
            if [ $? -ne 0 ]; then
                return_code=1
            fi
        done
    fi

    return $return_code
}

function run_benchmark {
    declare -i return_code=0

    pushd $TPC_DS_DIR > /dev/null

    for NUM_RUN in $(eval echo "{1..$NUM_RUNS}"); do
        hosts_file=$OUT_CONF/slaves

        declare -i run_exit_status=0

        CURRENT_RUN_DIR=$OUT/run-$NUM_RUN
        CURRENT_HIVE_LOG_DIR="$CURRENT_RUN_DIR/hive-logs"
        CURRENT_RUNTIME_FILENAME="run-times.csv"

        mkdir -p $CURRENT_HIVE_LOG_DIR

        # run commands on slaves, e.g. clear cache, sync time, clear previous profiler results
        if [[ $SYNC_TIME = true ]]; then
            common_setup_sync_time $LOCAL_MODE $hosts_file >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi
        if [[ $CLEAR_CACHE = true ]] && [[ $BEELINE = false ]]; then
            common_setup_clear_cache $LOCAL_MODE $hosts_file >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi
        if [[ $PROFILE = true ]] && [[ $BEELINE = false ]]; then
            common_setup_clear_profiler $LOCAL_MODE $hosts_file >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi
        if [[ $SECURE_MODE = true ]]; then
            common_setup_kinit
        fi
        if [[ $ENABLE_DSTAT = true ]]; then
            common_setup_start_dstat $LOCAL_MODE $hosts_file $CURRENT_RUN_DIR >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi
        if [[ $ENABLE_SS = true ]]; then
            common_setup_start_ss $LOCAL_MODE $hosts_file $CURRENT_RUN_DIR >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi

        # add header to run-times.csv
        echo "### REPORT ###: $CURRENT_RUN_DIR" > "$CURRENT_RUN_DIR/$CURRENT_RUNTIME_FILENAME"
        echo "$RUN_TIMES_HEADER" >> "$CURRENT_RUN_DIR/$CURRENT_RUNTIME_FILENAME"

        if [[ $ENABLE_NETSTAT = true ]]; then
          common_setup_netstat $LOCAL_MODE $hosts_file
        fi 

        # run all queries or a specific query 
        if [[ $RUN_QUERY = "all" ]]; then
            run_all_queries
            run_exit_status=$?
        else
            run_query "$TPC_DS_QUERY_DIR/query$RUN_QUERY.sql"
            run_exit_status=$?
        fi

        if [[ $ENABLE_NETSTAT = true ]]; then
          common_setup_netstat $LOCAL_MODE $hosts_file 
        fi 

        # collect data from slaves, e.g. profiler results, dstat, ss
        if [[ $ENABLE_DSTAT = true ]]; then
            common_setup_collect_dstat $LOCAL_MODE $hosts_file $CURRENT_RUN_DIR >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
            common_setup_clean_stat $LOCAL_MODE $hosts_file $OUT >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi
        if [[ $ENABLE_SS = true ]]; then
            common_setup_collect_ss $LOCAL_MODE $hosts_file $CURRENT_RUN_DIR >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
            common_setup_clean_stat $LOCAL_MODE $hosts_file $OUT >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi

        if [[ $SECURE_MODE = true ]]; then
            common_setup_kill_kinit >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1 >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi
        if [[ $PROFILE = true ]] && [[ $BEELINE = false ]]; then
            echo "Collecting profiler results is disabled"
            # common_setup_get_profiler_report $LOCAL_MODE $hosts_file $CURRENT_RUN_DIR/profile >> $CURRENT_HIVE_LOG_DIR/clear.txt 2>&1
        fi

        # delete run dir when --test and success
        if [ $run_exit_status -eq 0 ]; then
            if [ $DELETE_MODE = true ]; then
                rm -rf $CURRENT_RUN_DIR
            fi
        else
            # if one iteration fails, return non-zero result
            return_code=1
        fi
    done

    popd > /dev/null 

    return $return_code
}

function main {
    declare -i return_code=0

    hive_setup_parse_args_common $@
    parse_args $REMAINING_ARGS

    # initialize environment vars, setup output directory
    run_hive_ds_init

    pushd $BASE_DIR > /dev/null

    # update HADOOP_OPTS and HADOOP_CLASSPATH
    hive_setup_init_run_configs $OUT $LOCAL_MODE $AM_PROCESS $HIVE_METASTORE_PORT $LOG_LEVEL

    echo -e "\n# Running TPC-DS Hive-MR3 ($HIVE_REV, $TEZ_REV) #\n" >&2

    if [ $TEST_MODE = false ]; then
        common_setup_output_header
    fi

    run_benchmark
    return_code=$?

    common_setup_avg_time $OUT/run-results
    if [[ $NUM_RUNS -gt 1 ]]; then
        tail -n1 $OUT/run-results
    fi

    popd > /dev/null

    # delete run dir when --test and all runs were successful
    if [ $return_code = 0 ] && [ $DELETE_MODE = true ]; then
       rm -rf $OUT
    fi

    exit $return_code
}

main $@
