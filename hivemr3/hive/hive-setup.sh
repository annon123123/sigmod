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

# Check if file already been sourced
# Alternatively we could check return status of 'declare -F function_name' defined in script
if [ "${HIVE_SETUP_DEFINED:+defined}" ]; then
    exit 0;
fi
readonly HIVE_SETUP_DEFINED=true

function hive_setup_print_usage_hivesrc {
    echo " --hivesrc3                             Choose hive3-mr3 (based on Hive 3.1.2) (default)."
}

function hive_setup_print_usage_hiveconf {
    echo " --hiveconf <key>=<value>               Add a configuration key/value; may be repeated at the end."
}

function hive_setup_parse_args_common {
    HIVE_SRC_TYPE=3
    LOCAL_MODE=true
    CONF_TYPE=local

    while [[ -n $1 ]]; do
        case "$1" in
            --hivesrc3)
                HIVE_SRC_TYPE=3
                shift
                ;;
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
            --conf|--hiveconf)
                export HIVE_OPTS="$HIVE_OPTS ${@//--conf/--hiveconf}"
                shift 2
                ;;
            *)
                REMAINING_ARGS="$REMAINING_ARGS $1"
                shift
                ;;
        esac
    done

    if ! [[ $USE_HDP = true ]] && ! [[ $USE_K8S = true ]] && ! [[ $RUN_BEELINE = true ]] ; then
      export HIVE_OPTS="--hiveconf mr3.runtime=tez $HIVE_OPTS"
    fi
}

function hive_setup_init {
    HIVE_SRC=$HIVE3_SRC
    HIVE_REV=$HIVE3_REV
    HIVE_DATABASE_HOST=$HIVE3_DATABASE_HOST
    HIVE_METASTORE_HOST=$HIVE3_METASTORE_HOST
    HIVE_METASTORE_PORT=$HIVE3_METASTORE_PORT
    HIVE_DATABASE_NAME=$HIVE3_DATABASE_NAME
    HIVE_HDFS_WAREHOUSE=$HIVE3_HDFS_WAREHOUSE
    HIVE_METASTORE_LOCAL_PORT=$HIVE3_METASTORE_LOCAL_PORT
    HIVE_SERVER2_HOST=$HIVE3_SERVER2_HOST
    HIVE_SERVER2_PORT=$HIVE3_SERVER2_PORT

    HIVE_SRC_REV=$(common_setup_get_git_rev $HIVE_SRC)

    export HIVE_HOME=$HIVE_BASE_DIR/hivejar/apache-hive-$HIVE_REV-bin

    HIVE_LOCAL_DATA=$HIVE_BASE_DIR/hive-local-data

    MR3_CLASSPATH=$MR3_LIB_DIR/$MR3_TEZ_ASSEMBLY_JAR

    # HIVE uses $HIVE_LIB
    export HIVE_CONF_DIR=$HIVE_HOME/conf
    HIVE_JARS=$HIVE_HOME/lib
    HIVE_BIN=$HIVE_HOME/bin
    HCATALOG_DIR=$HIVE_HOME/hcatalog
    HCATALOG_CONF_DIR=$HCATALOG_DIR/etc/hcatalog
    HCATALOG_JARS=$HCATALOG_DIR/share/hcatalog
    export HIVE_CLASSPATH=$HIVE_MYSQL_DRIVER:$HIVE_CONF_DIR:$HCATALOG_CONF_DIR:$HIVE_JARS/*:$HCATALOG_JARS/*

    export PATH=$HIVE_BIN:$PATH

    HIVE_UNEXPECTED_EXCEPTIONS=(
       "EOFException"
       "ChecksumException"
       "IOException"
       "IllegalStateException"
       "ConnectException"
       "SemanticException"
       "TTransportException"
       "GSSException"
       "MetaException"
       "NoSuchMethodError" 
    )

    APPID_SEARCH_REGEX="Status: Running (Executing on .* with ApplicationID application_"
}

function hive_setup_init_conf {
    declare conf_type=$1

    HIVE_MR3_CONF_DIR=$BASE_DIR/conf/$conf_type/hive$HIVE_SRC_TYPE

    if ! [ -d "$HIVE_MR3_CONF_DIR" ]; then
      echo "ERROR: Not a directory, HIVE_MR3_CONF_DIR=$HIVE_MR3_CONF_DIR"
      exit 1;
    fi
}

function hive_setup_init_heapsize_mb {
    declare heapsize=$1

    export HADOOP_CLIENT_OPTS="-Xmx${heapsize}m $HADOOP_CLIENT_OPTS"
}

function hive_setup_check_hive_home {
    declare -i return_code=0

    # validate env.sh env vars
    if ! [ -d "$HIVE_HOME" ]; then
      return_code=1
    fi

    return $return_code
}

##
# Return Variables
# OUT  Directory script will output run files
# SCRIPT_START_TIME
##
function hive_setup_create_output_dir {
    declare base_dir=$1
    declare rev_num=$2

    SCRIPT_START_TIME=$(date +%s)
    declare time_stamp="$(common_setup_get_time $SCRIPT_START_TIME)"
    export OUT=$base_dir/hive-mr3-$rev_num-$time_stamp-$(uuidgen | awk -F- '{print $1}')
    mkdir -p $OUT > /dev/null 2>&1
    echo -e "Output Directory: \n$OUT\n"
}

function hive_setup_config_run_cluster {
        hdfs dfs -rm -r .Trash/*
}

# Usage:
# get_pid_by_port $port var_for_pid
# var_for_pid should be uninitialized and can be any name, (pid), $ is left off
#
function hive_setup_get_pid_by_port {
    declare port=$1
    #pass in var for ref where pid will be stored
    #pid=$2

    which ss > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "ss is not available. Please manually stop the process using port $port"
    else
        #declare found_pid="$(netstat -anp | grep -E ":$port.*LISTEN" | awk '{ print $7 }' | cut -d'/' -f1)"
        declare found_pid="$(ss -anpt | grep -E "LISTEN.*:$port" | awk '{ print $6 }' | cut -d',' -f2 | cut -d'=' -f2)"

        if [ -n "$found_pid" ]; then
            # use bash passed arg as reference
            #echo "found_pid=$found_pid"
            eval "$2=$found_pid"
            echo "found process $2 listening on port $port"
        else
            echo "No process listening on port $port"
        fi
    fi
}

function hive_setup_wait_for_pid_stop {
    declare pid=$1
    echo "Waiting for Process($pid) to stop..."
    while ps -p $pid; do sleep 1;done;
    echo "Process($pid) stopped."
}

function hive_setup_update_hadoop_opts {
    declare log_dir=$1
    declare local_mode=$2
    declare am_process=$3

    tez_setup_update_yarn_opts $local_mode

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

    #Hive does not use $YARN_OPTS, we need $HADOOP_OPTS instead
    export HADOOP_OPTS="$HADOOP_OPTS $YARN_OPTS"
}

function hive_setup_metastore_update_hadoop_opts {
   declare metastore_port=$1
   declare local_mode=$2

   export HADOOP_OPTS="$HADOOP_OPTS \
-Dhive.database.host=$HIVE_DATABASE_HOST \
-Dhive.metastore.host=$HIVE_METASTORE_HOST \
-Dhive.metastore.port=$metastore_port \
-Dhive.database.name=$HIVE_DATABASE_NAME \
-Dhive.local.data=$HIVE_LOCAL_DATA \
-Dhive.hdfs.warehouse.dir=$HIVE_HDFS_WAREHOUSE \
-Dhive.secure.mode=$SECURE_MODE \
-Dhive.metastore.keytab.file=$HIVE_METASTORE_KERBEROS_KEYTAB \
-Dhive.metastore.principal=$HIVE_METASTORE_KERBEROS_PRINCIPAL \
$YARN_OPTS"

    if [[ $SECURE_MODE = true ]]; then
        export HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.login=hybrid"
    fi
}

function hive_setup_server2_update_hadoop_opts {
   declare hiveserver2_port=$1
   declare local_mode=$2

   export HADOOP_OPTS="$HADOOP_OPTS \
-Dhive.server2.host=$HIVE_SERVER2_HOST \
-Dhive.server2.port=$hiveserver2_port \
-Dhive.server2.authentication.mode=$HIVE_SERVER2_AUTHENTICATION \
-Dhive.server2.keytab.file=$HIVE_SERVER2_KERBEROS_KEYTAB \
-Dhive.server2.principal=$HIVE_SERVER2_KERBEROS_PRINCIPAL"
}

function hive_setup_config_hadoop_classpath {
    export MR3_CLASSPATH=$MR3_CONF_DIR:$MR3_CLASSPATH
    # do not include HIVE_CLASSPATH which is added to HADOOP_CLASSPATH by hive command
    export HADOOP_CLASSPATH=$MR3_CLASSPATH:$TEZ_CLASSPATH:$HADOOP_CLASSPATH
}

function hive_setup_config_hive_logs {
    declare output_dir=$1
    declare log_filename=${2:-hive.log}

    mkdir -p $output_dir
    export HIVE_OPTS="--hiveconf hive.querylog.location=$output_dir $HIVE_OPTS"

    # Cf. HIVE-19886
    # YARN_OPTS should be updated because some LOG objects may be initialized before HiveServer2.main() calls 
    # LogUtils.initHiveLog4j(), e.g., conf.HiveConf.LOG. Without updating YARN_OPTS, these LOG objects send their output
    # to hive.log in a default directory, e.g., /tmp/gitlab-runner/hive.log, and we end up with two hive.log files.
    export YARN_OPTS="-Dhive.log.dir=$output_dir -Dhive.log.file=$log_filename"
}

function hive_setup_set_hive_opts {
    export HIVE_OPTS="$@ $HIVE_OPTS"
}

function hive_setup_set_hive_loglevel {
    declare hive_loglevel=$1
    export HIVE_OPTS="--hiveconf hive.root.logger=$hive_loglevel,DRFA,console $HIVE_OPTS"
}

function hive_setup_update_conf_dir {
    declare conf_dir=$1

    cp -r $HIVE_CONF_DIR/* $conf_dir
    cp -r $HCATALOG_CONF_DIR/* $conf_dir
    cp -r $HIVE_MR3_CONF_DIR/* $conf_dir

    rm -f $conf_dir/*.template

    export HIVE_CONF_DIR=$conf_dir
    export HIVE_CLASSPATH=$HIVE_CONF_DIR:$HIVE_CLASSPATH
}

##
# Initialize the output directory
##
function hive_setup_init_output_dir {
    declare local_mode=$1
    declare conf_type=$2
    declare output_dir=$3

    mkdir -p $output_dir > /dev/null 2>&1

    #create $OUT env var
    hive_setup_create_output_dir $output_dir $HIVE_SRC_REV

    common_setup_get_command > $OUT/command
    common_setup_get_git_info $MR3_SRC $OUT/mr3-dm-info
    common_setup_get_git_info $BASE_DIR $OUT/mr3-run-info
    common_setup_get_git_info $TEZ_SRC $OUT/tez-mr3-info
    common_setup_get_git_info $HIVE_SRC $OUT/hive-mr3-info

    OUT_CONF=$OUT/conf
    mkdir -p $OUT_CONF > /dev/null 2>&1

    cp $BASE_DIR/env.sh $OUT_CONF
    hadoop_setup_update_conf_dir $OUT_CONF $conf_type
    tez_setup_update_conf_dir $OUT_CONF $conf_type
    mr3_setup_update_conf_dir $OUT_CONF $conf_type
    hive_setup_update_conf_dir $OUT_CONF
    common_setup_update_conf_dir $OUT_CONF $conf_type

    if [[ $USE_K8S = true ]]; then
        kubernetes_setup_update_conf_dir $OUT_CONF
    fi
}

function hive_setup_get_yarn_report_from_file {
    declare file=$1
    declare app_status_file=$2
    declare yarn_log_dir=$3

    declare -i return_code=0
    declare app_id_found=false

    if [[ ! -z "${file// }" ]]; then
        while read -r appId_str; do
            # Limit scope of global var
            declare YARN_APPLICATION_ID
            hadoop_setup_get_yarn_app_id_from_line "$appId_str"
            if [ $? -ne 0 ]; then
                printf "\n# ERROR: Failed to get appid from $file #\n\n"
                continue
            fi
            app_id_found=true
            declare appId=$YARN_APPLICATION_ID
            echo "ApplicationId: $appId"
            declare yarn_log_app_dir="$yarn_log_dir/$appId"

            # Skip if yarn logs already retrieved
            if [ ! -d "$yarn_log_app_dir" ]; then
                hadoop_setup_get_yarn_report $appId $CURRENT_RUN_DIR $yarn_log_app_dir $app_status_file
            fi
        done < <(grep "$APPID_SEARCH_REGEX" $file)
    fi

    # Indicate a failure to find an Application ID to get yarn report
    # TODO: have hadoop_setup_get_yarn_report return code for successful yarn logs retrieved
    [ $app_id_found = false ] && let return_code=1

    return $return_code
}

function hive_setup_report_run_results {
    declare run_name=$1
    declare -i run_num=$2
    declare -i job_status_code=$3
    declare -i error_status_code=$4
    declare start_time_epoch=$5
    declare run_time=$6
    declare run_dir=$7
    declare ex_file=$8

    # print report
    if [ $job_status_code = 0 ] && [ $error_status_code = 0 ]; then
        if [ ${ex_file:+defined} ] && [ -e "$ex_file" ]; then
            rm -rf $ex_file
        fi
    else
        chmod -R 777 $run_dir
        if [ ${ex_file:+defined} ] && [ -s "$ex_file" ]; then
            printf "##########\n PROBLEMS \n##########\n"
            cat $ex_file
        fi
    fi

    # pipe epoch time to common date func to format
    declare start_time="$(common_setup_get_time $start_time_epoch)"

    common_setup_output_status $job_status_code \
        "$error_status_code" \
        "$start_time" \
        "$run_time" \
        "$run_name" \
        "$run_num" \
        "$run_dir" | tee -a $OUT/run-results
}

function hive_setup_check_exceptions {
    declare log_file=$1
    declare ex_file=$2

    declare -a custom_exceptions

    declare -i return_code=0

    ADDITIONAL_UNEXPECTED_EXCEPTIONS=("${HIVE_UNEXPECTED_EXCEPTIONS[@]}")
    common_setup_check_exception "$ex_file" "$log_file" > /dev/null 2>&1
    ADDITIONAL_UNEXPECTED_EXCEPTIONS=()

    return_code=$?

    #declare num_start_hh=$(grep -rn "Started Service: HeartbeatHandler" $log_file | wc -l)
    #declare num_stop_hh=$(grep -rn "Stopped Service: HeartbeatHandler" $log_file | wc -l)
    #if [[ ! $num_start_hh = $num_stop_hh ]]; then
    #    echo "Started Service: HeartbeatHandler = $num_start_hh" >> $ex_file
    #    echo "Stopped Service: HeartbeatHandler = $num_stop_hh" >> $ex_file
    #    return_code=1
    #fi

    return $return_code
}

function hive_setup_init_run_configs {
    declare out_dir=$1
    declare local_mode=$2
    declare am_process=$3
    declare metastore_port=$4
    declare log_level=$5

    hive_setup_update_hadoop_opts $out_dir $local_mode $am_process

    YARN_OPTS=""  # YARN_OPTS already consumed in hive_setup_update_hadoop_opts
    hive_setup_metastore_update_hadoop_opts $metastore_port $local_mode

    if [ $local_mode = false ]; then
        hive_setup_config_run_cluster > /dev/null 2>&1
    fi

    hive_setup_config_hadoop_classpath

    #hive_setup_set_hive_loglevel "$log_level"
}

##
# executes a given command, and stores output in given
# execute_cmd(cmd, outputFile, boolean silent(default: true))
# Returns exit code from eval statement
#
# Result variables
#  EXEC_EXIT_CODE
#  EXEC_TIMED_RUN_START
#  EXEC_TIMED_RUN_STOP
#  EXEC_TIMED_RUN_TIME
##
function hive_setup_exec_cmd {
    declare cmd=$1
    declare out_file=$2
    declare silent=$3
    declare combine=${4:-true}

    declare silent_opts=">> $out_file 2>&1"
    declare combined_out_file="$(dirname $out_file)/out-all.txt"
    declare combine_out_opts="2>&1 | tee -a $combined_out_file"

    #Output env to command file
    echo -e "\n### Environment variables before running hive command ###\n" >> $OUT/env
    env >> $OUT/env

    EXEC_EXIT_CODE=0

    echo "Output file: $out_file" >> $out_file

    if [ $combine = true ]; then
      cmd+=" $combine_out_opts"
    fi 

    [ $silent = true ] && cmd+=" $silent_opts" || cmd+=""

    cmd="set -o pipefail; $cmd"

    echo "$cmd" >> $out_file

    EXEC_TIMED_RUN_TIME=0
    EXEC_TIMED_RUN_START=$(date -u +%s%3N)
    eval "$cmd"
    EXEC_EXIT_CODE=$?
    EXEC_TIMED_RUN_STOP=$(date -u +%s%3N)
    elapsed_raw=$((EXEC_TIMED_RUN_STOP-$EXEC_TIMED_RUN_START))
    EXEC_TIMED_RUN_TIME="$((elapsed_raw / 1000)).$((elapsed_raw % 1000))"

    return $EXEC_EXIT_CODE
}

##
# Times a given command, calls hive_setup_exec_cmd
# execute_cmd_timed(cmd, outputFile, boolean silent(default: true))
# Returns exit code EXEC_EXIT_CODE from hive_setup_exec_cmd
#
# Result variables
#  EXEC_EXIT_CODE
#  EXEC_TIMED_RUN_START
#  EXEC_TIMED_RUN_STOP
#  EXEC_TIMED_RUN_TIME
##
function hive_setup_exec_cmd_timed {
    declare cmd=$1
    declare out_file=$2
    declare silent=$3
    declare combine=${4:-true}

    hive_setup_exec_cmd "$cmd" $out_file $silent $combine
    
    return $EXEC_EXIT_CODE
}
