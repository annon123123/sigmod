#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function hadoop_setup_print_usage_get_yarn_logs {
    echo " --get-yarn-logs                        Get logs from Yarn application."
}

function hadoop_setup_init {
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export YARN_CONF_DIR=$HADOOP_CONF_DIR
    export HADOOP_CLASSPATH=$HADOOP_CONF_DIR:$HADOOP_CLASSPATH
    export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
}

function hadoop_setup_update_conf_dir {
    conf_dir=$1
    conf_type=$2

    MAPREDUCE_CONF_DIR=$BASE_DIR/conf/$conf_type/mapreduce

    # copy the original conf files from cluster
    cp -r $HADOOP_CONF_DIR/* $conf_dir 2> /dev/null
    cp -r $YARN_CONF_DIR/* $conf_dir 2> /dev/null

    rm -f $conf_dir/*.template
    rm -f $conf_dir/*.example
    rm -f $conf_dir/*.cmd

    # override mapred-site.xml by the one in the configuration directory
    cp -r $MAPREDUCE_CONF_DIR/* $conf_dir

    export HADOOP_CONF_DIR=$conf_dir
    export YARN_CONF_DIR=$HADOOP_CONF_DIR
    export HADOOP_CLASSPATH=$HADOOP_CONF_DIR:$HADOOP_CLASSPATH
}

#
# for getting yarn logs
#

##
# Return Variables
# YARN_APPLICATION_ID contains the parsed application id
# return code 0 for successfully parsing application id
##
function hadoop_setup_get_yarn_app_id_from_line {
    declare raw_app_id_line=$1
    if [[ $raw_app_id_line =~ (application_[0-9]+_[0-9]+) ]]; then
        YARN_APPLICATION_ID="${BASH_REMATCH[1]}"
        return 0
    fi

    return 1
}

function hadoop_setup_get_yarn_report {
    appid=$1
    out_dir=$2
    log_dir=$3
    stat_file=$4

    if [[ $IGNORE_GET_YARN_REPORT = true ]]; then
        return 0
    fi

    # get application status file
    yarn application -status $appid > $stat_file 2>&1
    is_finished=$(grep "Final-State" $stat_file | grep -c UNDEFINED)
    grep "Final-State" $stat_file
    while [[ $is_finished -gt 0 ]]; do
        sleep 1
        yarn application -status $appid > $stat_file 2>&1
        is_finished=$(grep "Final-State" $stat_file | grep -c UNDEFINED)
        grep "Final-State" $stat_file
    done
    cat $stat_file

    # get time
    start_time=$(cat $stat_file | grep "Start-Time" | awk '{print $3}')
    finish_time=$(cat $stat_file | grep "Finish-Time" | awk '{print $3}')
    if [[ $start_time = "" ]] || [[ $finish_time = "" ]]; then
      run_time=0
    else
      run_time=$((($finish_time-$start_time)/1000))
    fi
    echo "RUNNING_TIME: $run_time" | tee -a $stat_file

    # get log
    is_log_aggregation_enabled=$(grep -A1 "yarn.log-aggregation-enable" $YARN_CONF_DIR/yarn-site.xml | tail -n1 | grep -c true)
    if [[ $is_log_aggregation_enabled = 0 ]]; then
        echo "WARNING: Cannot get log files because yarn.log-aggregation-enable is false in yarn-site.xml"
    else
        # wait until log aggregation finished, then get log
        max_tries=${MAX_GET_YARN_REPORT_TRIES:-10}
        max_num_validation=${MAX_NUM_VALIDATION_GET_YARN_REPORT_TRIES:-2}
        sleep_time=${SLEEP_TIME_GET_YARN_REPORT_TRIES:-5}

        tmp_log_file=$out_dir/tmp-logs-$appid
        touch $tmp_log_file
        try=0
        num_validation=0
        while [[ $try -lt $max_tries ]]; do
            try=$((try+1))

            oldsize=0
            if [[ -e $tmp_log_file ]]; then
                mv $tmp_log_file $tmp_log_file.old
                oldsize=$(du -b $tmp_log_file.old | cut -f 1)
            fi

            yarn logs -applicationId $appid > $tmp_log_file 2>&1
            newsize=$(du -b $tmp_log_file | cut -f 1)

            if [[ $newsize -gt 0 ]] && [[ $newsize = $oldsize ]]; then
                num_validation=$((num_validation+1))
                if [[ $num_validation -gt $max_num_validation ]];  then
                    break
                fi
            else
                num_validation=0
            fi

            echo "!!! sleep $sleep_time seconds to wait for log aggregation !!!"
            sleep $sleep_time
        done

        # split log
        mkdir -p $log_dir
        pushd $log_dir > /dev/null

        container_regex="Container: container_.* on "
        num_containers=$(grep "$container_regex" $tmp_log_file | wc -l)

        if [[ $num_containers -gt 0 ]]; then
            csplit -s -z -b %d $tmp_log_file "/$container_regex/" '{*}'  # split logs into xx0, xx1, ...

            for j in $(eval echo "{1..$num_containers}"); do
                container_name=$(head -n1 xx$j | awk '{print $2}')
                if [[ ! -f $container_name ]]; then
                    mv xx$j $container_name  # rename log files to container_0, container_1, ...
                else
                    cat xx$j >> $container_name
                    rm -f xx$j
                fi
            done
        fi
        popd > /dev/null

        # get num containers
        echo "NUM_CONTAINERS: $num_containers" | tee -a $stat_file

        rm -f $tmp_log_file*
    fi

    echo "OUTPUT DIR: $out_dir"
}

#
# for share inputs
#

function hadoop_setup_get_share_input {
    input=$1

    if [[ $SHARE_INPUT = "true" ]]; then
        echo $SHARE_INPUT_DIR/$input
    else
        echo $input
    fi
}

#
# for running yarn and MapReduce examples
#

function hadoop_setup_run_yarn {
    declare jar_file=$1
    declare job_args="${@:2}"

    echo -e "\n### Environment variables before running yarn command ###\n" >> $BASE_OUT/env
    env >> $BASE_OUT/env

    yarn --loglevel $LOG_LEVEL jar $jar_file $job_args
}

function hadoop_setup_run_examples {
    job_opts=$1
    job_args=$2

    hadoop_setup_run_yarn $MR3_LIB_DIR/hadoop-mapreduce-examples*.jar $job_opts $job_args
}

##
# Checks the Yarn Application state for Cluster jobs
# Returns code 0 if app succeeded, otherwise non-zero
##
function hadoop_setup_check_yarn_app_state {
    declare client_log_file=$1

    if [[ $IGNORE_GET_YARN_REPORT = true ]]; then
        return 0
    fi

    # for CrossDagReuseWithLocalResources
    if [[ $(grep "TEST FAILED" $client_log_file) ]]; then
        return 1
    fi

    if [[ $(grep "State : FINISHED" $client_log_file) ]] && \
        [[ $(grep "Final-State : SUCCEEDED" $client_log_file) ]]; then
        return 0
    fi
    return 1
}
