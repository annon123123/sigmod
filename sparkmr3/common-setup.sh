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

# only for 'source'
HADOOP_BASE_DIR=$BASE_DIR/hadoop
TEZ_BASE_DIR=$BASE_DIR/tez
MR3_BASE_DIR=$BASE_DIR/mr3
HIVE_BASE_DIR=$BASE_DIR/hive
ATS_BASE_DIR=$BASE_DIR/ats
K8S_BASE_DIR=$BASE_DIR/kubernetes
GITLAB_BASE_DIR=$BASE_DIR/gitlab
TOOLS_BASE_DIR=$BASE_DIR/tools
LOG_BASE_DIR=$BASE_DIR/log
RELEASE_BASE_DIR=$BASE_DIR/release
SPARK_BASE_DIR=$BASE_DIR/spark
MAPREDUCE_BASE_DIR=$BASE_DIR/mapreduce

function common_setup_print_usage_common_options {
    echo " -h/--help                              Print the usage."    
    echo " --delete                               Delete the output directory if the script succeeds without error."
}

function common_setup_print_usage_conf_mode {
    echo " --local                                Run jobs with configurations in conf/local/ (default)."
    echo " --cluster                              Run jobs with configurations in conf/cluster/."
    echo " --tpcds                                Run jobs with configurations in conf/tpcds/."
}

function common_setup_init {
    mkdir -p $LOG_BASE_DIR 2> /dev/null

    MR3_RUN_REV=$(common_setup_get_git_rev $BASE_DIR)

    UNEXPECTED_EXCEPTIONS=(
        "MR3UncheckedException"
        "AssertionError"
        "IllegalArgumentException"
        "NotImplementedError"
        "IndexOutOfBoundsException"
        "NoSuchElementException"
        "YarnException"
        "OutOfMemoryError"
        "StackOverflowError"
        "ClassCastException"
        "NoSuchMethodError"
        "Rescheduling for.*with failed/killed = "
        "non-zero exit code 137"
        "All datanodes DatanodeInfoWithStorage.*are bad. Aborting..."
        "token.*is expired"
    )

    IGNORED_EXCEPTIONS=(
        "MR3UncheckedException: Intended for test"
        "java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState.LAUNCHED_UNMANAGED_SAVING"  # YARN-4411, Hadoop 2.7.1
        "XX:OnOutOfMemoryError"
        "Rescheduling for.*with failed/killed = 0/0"
        "NullPointerException"
        "NullPointerException: null"
        "com.google.protobuf.ServiceException"
        "requirement failed: DAG LocalResources incompatible with existing LocalResources"
        "requirement failed: Additional AM LocalResources incompatible with existing LocalResources"
        "requirement failed: ContainerGroup.localResources unequal"
        "requirement failed: additionalAmLocalResources incompatible with existing LocalResources"
        "OutOfMemoryError: test"
    )

    # For the user to add more exceptions. Note that the user should clear these arrays after calling common_setup_check_exception.
    ADDITIONAL_UNEXPECTED_EXCEPTIONS=()
    ADDITIONAL_IGNORED_EXCEPTIONS=()

    if [[ $HONEST_PROFILER = true ]] || [[ $JAVA_FLIGHT_RECORDER = true ]] || [[ $YOURKIT = true ]]; then
        PROFILE=true
    else
        PROFILE=false
    fi
}

function common_setup_update_conf_dir {
    conf_dir=$1
    conf_type=$2

    COMMON_CONF_DIR=$BASE_DIR/conf/$conf_type

    # copy only files from $conf_dir and do not copy from subdirectories
    cp $COMMON_CONF_DIR/* $conf_dir 2>&1 | grep -v "omitting directory"
}

# be careful when passing arguments, with-$ for normal vars, without-$-but-with-@ for array vars
# Ex: common_setup_check_exception $path1 $path2 arr1[@] arr2[@]
function common_setup_check_exception {
    declare ex_file=$1
    declare log_path=$2

    exceptions=(
        "${UNEXPECTED_EXCEPTIONS[@]}"
        "${ADDITIONAL_UNEXPECTED_EXCEPTIONS[@]}"
    )
    ignored_exceptions=(
        "${IGNORED_EXCEPTIONS[@]}"
        "${ADDITIONAL_IGNORED_EXCEPTIONS[@]}"
    )

    grep_match=""
    for ex in "${exceptions[@]}"; do
        [[ -z $ex ]] || grep_match="$grep_match -e \"$ex\""
    done

    grep_ignore=""
    for ex in "${ignored_exceptions[@]}"; do
        [[ -z $ex ]] || grep_ignore="$grep_ignore | grep -v \"$ex\""
    done

    grep_exclude="--exclude=*.jar --exclude=blk_*"

    # note that each exception in $grep_ignore is after a pipe '|' command
    cmd="grep -Hrn $log_path $grep_match $grep_exclude $grep_ignore | common_setup_format_grep"
    mkdir -p /tmp/$USER > /dev/null 2>&1
    echo $cmd > /tmp/$USER/ex_cmd
    eval $cmd | tee -a $ex_file

    if [[ -s $ex_file ]]; then
        return 1
    fi
    return 0
}

# given two string, check where the number occurences of each string is equal or not
function common_setup_check_pattern {
    declare ex_file=$1
    declare log_path=$2
    start_pattern=$3
    stop_pattern=$4

    log_file_pattern="\.log"
    find $log_path | grep "$log_file_pattern" | while read log_file; do
        num_start=$(grep -c "$start_pattern" $log_file)
        num_stop=$(grep -c "$stop_pattern" $log_file)

        if [[ ! $num_start = $num_stop ]]; then
            echo "Problem of $start_pattern and $stop_pattern in $log_file" | tee -a $ex_file
            echo "$start_pattern == $num_start" | tee -a $ex_file
            echo "$stop_pattern == $num_stop" | tee -a $ex_file
        fi
    done
}

# get the current time
# optionally takes a time in seconds from epoch and returns a formatted time
function common_setup_get_time {
    declare epoch_time=$1

    #[[ $epoch_time = "???" ]] && echo "???" ||
    date +"%Y-%m-%d-%H-%M-%S" ${epoch_time:+"-d @$epoch_time"}
}

function common_setup_avg_time {
    results_file=$1

    grep "SUCCEEDED, NONE" $results_file | awk -F',' -v max=0 -v min=10000 '{ print $5; sum += $5; n++; sumX2 += (($5)^2); if($5>max){max=$5}; if($5<min){min=$5} } END { if (n > 1) { avg = sum / n; sd = sqrt(sumX2 / (n - 1) - 2*avg*(sum / (n - 1)) + ((n * (avg^2))/(n - 1)));  printf "\n\n# Average: %.2f, Standard Deviation: %.2f, max: %.2f, min: %.2f (%d runs succeeded with no error) #\n", avg, sd, max, min, n } else if(n == 1) printf "\n\n# Single run: %.2f #\n", sum ; else printf "\n\n# No run #\n" }' >> $results_file
}

function common_setup_get_run_time_from_log {
    log_file=$1
    start_line_pattern=$2
    end_line_pattern=$3
    date_position_in_line=$4
    time_position_in_line=$5

    start_datetime=$(grep -h "$start_line_pattern" $log_file* | head -n1 |\
                     awk '{print $'$date_position_in_line' " " $'$time_position_in_line'}' | sed 's/\//-/g')
    start_sec=$(date -u -d "$start_datetime" +"%s.%N")

    end_datetime=$(grep -h "$end_line_pattern" $log_file* | tail -n1 |\
               awk '{print $'$date_position_in_line' " " $'$time_position_in_line'}' | sed 's/\//-/g')
    end_sec=$(date -u -d "$end_datetime" +"%s.%N")

    printf "%.3f" $(echo "$end_sec - $start_sec" | bc)
}

#
# for git
#

function common_setup_get_git_rev {
    git_dir=$1

    if [[ -d "$git_dir/.git" ]]; then
        pushd $git_dir > /dev/null
        git log --oneline -1 | awk '{print $1}'
        popd > /dev/null
    else
        echo ""
    fi
}

function common_setup_get_git_info {
    git_dir=$1
    git_out_file=$2

    if [[ -d "$git_dir/.git" ]]; then
        pushd $git_dir > /dev/null
        {
            git log --graph --decorate -1
            git status
            git diff
        } > $git_out_file
        popd > /dev/null
    fi
}

function common_setup_get_git_branch_names {
    git_dir=$1
    commit_id=$2

    if [[ -d "$git_dir/.git" ]]; then
        pushd $git_dir > /dev/null
        git branch -a --contains $commit_id | grep remotes | grep -v HEAD | awk -F/ '{print $3}'
        popd > /dev/null
    fi
}

#
# for clearing cache, updating mr3-run, clearing profiler results in remote machines
#

function common_setup_clear_cache {
    local_mode=$1
    hosts_file=$2

    echo "Clearing cache of this machine (require sudo permission)"
    sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1

    if [[ $local_mode = false ]]; then
        echo "Clearing cache of remote machines (require passwordless ssh, and sudo permission)"
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    ssh -n -ttt $host "sync && echo 3 | sudo tee /proc/sys/vm/drop_caches" > /dev/null 2>&1
                } &
            fi
        done < $hosts_file
        wait
        echo "Finished clearing cache"
    fi
}

function common_setup_sync_time {
    local_mode=$1
    hosts_file=$2

    echo "Sync time in this machine (require sudo permission) $hosts_file"
    sudo ntpdate -u pool.ntp.org > /dev/null 2>&1

    if [[ $local_mode = false ]]; then
        echo "Sync time remote machines (require passwordless ssh, and sudo permission)"
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    ssh -n -ttt $host "sudo ntpdate -u pool.ntp.org" > /dev/null 2>&1
                } &
            fi
        done < $hosts_file
        wait
        echo "Finished syncing time"
    fi
}

function common_setup_kill_sleep {
    ps fjx | grep sleep | awk '$1==1 {print $0}' | awk '$10=="sleep" {print $0}' | awk '{print $2}' | while read pid; do kill -9 $pid; done
}

function common_setup_start_dstat {
    local_mode=$1
    hosts_file=$2
    out_dir=$3

    ps fux | grep dstat | grep -v 'grep dstat' | awk '{print $2}' | while read pid; do kill -9 $pid; done
    common_setup_kill_sleep

    dstat_dir=$out_dir/dstat
    mkdir -p $dstat_dir
    pid_dir=$dstat_dir/pid
    mkdir -p $pid_dir

    disks=$(df | grep sd | awk -F/ '{print substr($3,0,3)}' | sort -u | paste -s -d',')
    dstat_cmd="dstat -cmnd -D total,$disks"

    dstat_out=$dstat_dir/$(hostname)-client
    rm -f $dstat_out*
    ssh $(hostname) "nohup $dstat_cmd --output $dstat_out.csv > $dstat_out.txt 2>&1 & "'echo $!' > $pid_dir/$(hostname).pid

    if [[ $local_mode = false ]]; then
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    ssh -n $host "ps fux | grep dstat | grep -v 'grep dstat'" | awk '{print $2}' | while read pid; do ssh -n $host "kill -9 $pid"; done

                    disks=$(ssh -n $host "df" | grep sd | awk -F/ '{print substr($3,0,3)}' | sort -u | paste -s -d',')
                    dstat_cmd="dstat -cmnd -D total,$disks"

                    dstat_out=$dstat_dir/$host-slave
                    rm -f $dstat_out*
                    ssh -n $host "mkdir -p $dstat_dir; nohup $dstat_cmd --output $dstat_out.csv > $dstat_out.txt 2>&1 & "'echo $!' > $pid_dir/$host.pid
                } &
            fi
        done < $hosts_file
        wait
    fi
}

function common_setup_collect_dstat {
    local_mode=$1
    hosts_file=$2
    out_dir=$3

    dstat_dir=$out_dir/dstat
    pid_dir=$dstat_dir/pid

    kill -9 $(cat $pid_dir/$(hostname).pid)

    if [[ $local_mode = false ]]; then
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    pid=$(cat $pid_dir/${host}.pid)
                    ssh -n $host "kill -9 $pid"
                    scp -q $host:$dstat_dir/* $dstat_dir
                } &
            fi
        done < $hosts_file
        wait
    fi

    rm -rf $pid_dir
}

function common_setup_start_ss {
    local_mode=$1
    hosts_file=$2
    out_dir=$3

    interval_sec=20
    shuffle_port=13562

    ss_dir=$out_dir/ss
    mkdir -p $ss_dir
    pid_dir=$ss_dir/pid
    mkdir -p $pid_dir

    ps fux | grep /usr/sbin/ss | grep -v 'grep /usr/sbin/ss' | awk '{print $2}' | while read pid; do kill -9 $pid; done
    common_setup_kill_sleep

    ss_cmd="echo; date; /usr/sbin/ss -s; /usr/sbin/ss -a | grep $shuffle_port"
    ssh $(hostname) "nohup sh -c 'while :; do $ss_cmd; sleep $interval_sec; done' > $ss_dir/$(hostname)-client.txt 2>&1 & "'echo $!' > $pid_dir/$(hostname).pid

    if [[ $local_mode = false ]]; then
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    ssh -n $host "ps fux | grep /usr/sbin/ss | grep -v 'grep /usr/sbin/ss'" | awk '{print $2}' | while read pid; do ssh -n $host "kill -9 $pid"; done
                    ss_cmd="echo; date; /usr/sbin/ss -s; /usr/sbin/ss -a | grep $shuffle_port"
                    ssh -n $host "mkdir -p $ss_dir; nohup sh -c 'while :; do $ss_cmd; sleep $interval_sec; done' > $ss_dir/$host-slave.txt 2>&1 & "'echo $!' > $pid_dir/$host.pid
                } &
            fi
        done < $hosts_file
        wait
    fi
}

function common_setup_collect_ss {
    local_mode=$1
    hosts_file=$2
    out_dir=$3
    dir_to_delete=$4

    ss_dir=$out_dir/ss
    pid_dir=$ss_dir/pid

    kill -9 $(cat $pid_dir/$(hostname).pid)

    if [[ $local_mode = false ]]; then
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    pid=$(cat $pid_dir/${host}.pid)
                    ssh -n $host "kill -9 $pid; cd $ss_dir; tar zcf ../$host.tar.gz *"
                    scp -q $host:$ss_dir/../$host.tar.gz $ss_dir
                    ssh -n $host "rm -rf $dir_to_delete"
                } &
            fi
        done < $hosts_file
        wait
    fi

    rm -rf $pid_dir
}

function common_setup_netstat {
    local_mode=$1
    hosts_file=$2

    if [[ $local_mode = false ]]; then
        while read host; do
            if [[ ! -z $host ]]; then
                ssh -n $host "echo $host; netstat -s | grep overflow"
            fi
        done < $hosts_file
        wait
    fi
}

function common_setup_clean_stat {
    local_mode=$1
    hosts_file=$2
    dir_to_delete=$3

    if [[ $local_mode = false ]]; then
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    ssh -n $host "rm -rf $dir_to_delete"
                } &
            fi
        done < $hosts_file
        wait
    fi
}

function common_setup_get_profiler_options {
    profile_dir=$1

    # Not that in non-secured clusters, container is run by user yarn.
    HONEST_OPTS="-agentpath:$TOOLS_BASE_DIR/honest-profiler/liblagent.so=interval=7,logPath=$profile_dir/\$CONTAINER_ID.hpl"
    JFR_OPTS="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=loglevel=info -XX:StartFlightRecording=name=profile,settings=profile,dumponexit=true,filename=$profile_dir/\$CONTAINER_ID.jfr"
    YOURKIT_OPTS="-agentpath:$TOOLS_BASE_DIR/yourkit/bin/linux-x86-64/libyjpagent.so=dir=$profile_dir,logdir=$profile_dir,sampling,monitors,alloceach=10,allocsizelimit=4096,allocsampled"
}

function common_setup_clear_profiler {
    local_mode=$1
    hosts_file=$2

    echo "Clearing old profiler results in this machine"
    sudo rm -rf $PROFILE_DIR_MASTER/*
    sudo mkdir -p $PROFILE_DIR_MASTER > /dev/null 2>&1
    sudo chmod 777 $PROFILE_DIR_MASTER

    if [[ $local_mode = false ]]; then
        echo "Clearing old profiler results in remote machines (require passwordless ssh)"
        while read host; do
            if [[ ! -z $host ]]; then
                ssh -n -ttt $host "sudo rm -rf $PROFILE_DIR_WORKER/*; sudo mkdir -p $PROFILE_DIR_WORKER; sudo chmod 777 $PROFILE_DIR_WORKER" > /dev/null 2>&1 &
            fi
        done < $hosts_file
        wait
        echo "Finished clearing old profiler results"
    fi
}

function common_setup_get_profiler_report {
    local_mode=$1
    hosts_file=$2
    out_profile=$3

    mkdir -p $out_profile

    echo "Collecting profiler results from this machine"
    cp $PROFILE_DIR_MASTER/* $out_profile > /dev/null 2>&1

    if [[ $local_mode = false ]]; then
        echo "Collecting profiler results from remote machines (require passwordless ssh)"
        while read host; do
            if [[ ! -z $host ]]; then
                {
                    old_size=-1
                    new_size=0
                    while [[ $new_size -ne $old_size ]]; do
                        sleep 5
                        old_size=$new_size
                        new_size=$(ssh -n $host "du -s $PROFILE_DIR_WORKER/ | awk '{print \$1}'")
                    done
                    scp -q $host:$PROFILE_DIR_WORKER/* $out_profile > /dev/null 2>&1
                } &
            fi
        done < $hosts_file
        wait
    fi
}

#
# for xml file
#

function common_setup_update_conf_xml_file {
    file=$1
    key=$2
    value=$3

    sed -i "/<name>$key<\/name>/!b;n;c\ \ \ \ <value>$value<\/value>" $file
}

#
# other
#

function common_setup_get_command {
    ps aux | grep $$ | grep -v grep
}

# formats grep output for multi-file grep searches by printing each file name once
#
# Example:
# grep -Hnr "print_usage" * | common_setup_format_grep
#  hive/compile-hive.sh:
#    17:function print_usage {
#    34:  print_usage
#    48:        print_usage
#
function common_setup_format_grep {
    awk '$0=="--" {print; next} {fname=line=$0; sub(/:.*/,":",fname); sub(fname,"  ",line)} \
         fname!=oldfname {print fname; oldfname=fname} {print line}'
}

function common_setup_kinit {
    if [[ $(ps fux | grep kinit | grep -v 'grep kinit' | wc -l) = 0 ]]; then
        ssh $(hostname) "nohup sh -c 'while :; do kinit $USER_PRINCIPAL -k -t $USER_KEYTAB; sleep $KINIT_RENEWAL_INTERVAL; done' > /dev/null 2>&1 &"
    fi
}

function common_setup_kill_kinit {
    ps fux | grep kinit | grep -v 'grep kinit' | awk '{print $2}' | while read pid; do kill -9 $pid; done
    common_setup_kill_sleep
}

# validation utilities
function common_setup_check_integer {
  declare num=$1

  if [[ "$num" =~ ^[-]?[0-9]+$ ]]; then
      return 0
  fi

  return 1
}

# logging/output utilities
function common_setup_output_header {
    echo -e "Job Status, Error Status, Hostname, Start Time, Runtime (seconds), Job Name, Run Number, Directory"
}

function common_setup_output_status {
    declare -i job_status_code=$1
    declare -i error_status_code=$2
    declare start_time=$3
    declare run_time=$4
    declare job_name=$5
    declare run_num=$6
    declare run_dir=$7

    # get status
    declare job_status
    [ $job_status_code == 0 ] && job_status="SUCCEEDED" || job_status="FAILED"

    # get run status
    declare error_status
    [ $error_status_code == 0 ] && error_status="NONE" || error_status="ERROR"

    declare output_line="$job_status, $error_status, $(hostname), $start_time, $run_time, $job_name, $run_num"
    if [ -n "$run_dir" ]; then
        output_line+=", $run_dir"
    fi

    echo -e "$output_line"
}

function common_setup_log {
    declare function_name=$1
    declare message=$2
    declare script_name="$(basename "$0")"

    echo -e "$script_name:$function_name() $message"
}

# prints a WARNING message 
#
# $1, calling Function (access func stack: ${FUNCNAME[0]})
# $2, Message to print
#
# Example: error "Strange stuff happened" print_usage
#
function common_setup_log_warning {
   declare function_name=$1 #${FUNCNAME[1]} for prev. function
   declare message=$2

   common_setup_log $function_name "WARNING: $message"
}

# Prints an ERROR message 
#
# $1, calling Function (access func stack: ${FUNCNAME[0]})
# $2, Message to print
#
function common_setup_log_error {
    declare function_name=$1 #${FUNCNAME[1]} for prev. function
    declare message=$2

    common_setup_log $function_name "ERROR: $message"
}

# extracts key and value from key=value
# returns 0 if both key and value are retrieved, otherwise non-zero
#
# variables defined:
#   OPT_KEY
#   OPT_VALUE
#
function common_setup_get_key_value {
    declare kvArg=$1
    declare del=${2:-"="}

    if [[ $kvArg =~ ([^$del]+)$del(.+) ]]; then
        OPT_KEY=${BASH_REMATCH[1]}
        OPT_VALUE=${BASH_REMATCH[2]}
        return 0
    fi

    return 1
}

function common_setup_get_random {
    declare from=$1
    declare to=$2

    if [[ $to -gt $from ]]; then
        echo $((from + RANDOM % (to + 1 - from)))
    else
        echo $from
    fi
}

function clean_lib {
    mr3_setup_init
    tez_setup_init
    rm -f $MR3_LIB_DIR/$MR3_TEZ_ASSEMBLY_JAR
    rm -f $MR3_JARS/$MR3_CORE_JAR
    rm -f $MR3_JARS/$MR3_TEZ_JAR
    rm -rf $TEZ_LIB_BASE_DIR/tezjar/*
    rm -rf $HIVE_BASE_DIR/hivejar/*
}

function clean_spark_lib {
    mr3_setup_init
    spark_setup_init
    rm -f $MR3_LIB_DIR/$MR3_SPARK_ASSEMBLY_JAR
    rm -f $MR3_JARS/$MR3_CORE_JAR
    rm -f $MR3_JARS/$MR3_SPARK_JAR
    rm -rf $SPARK_MR3_LIB_DIR/$SPARK_MR3_ASSEMBLY_JAR
}

#
# Kubernetes
#

function kubernetes_setup_update_conf_dir {
    conf_dir=$1

    k8s_conf=$conf_dir/kubernetes
    mkdir -p $k8s_conf > /dev/null 2>&1

    cp $K8S_BASE_DIR/env.sh $k8s_conf
    cp -r $K8S_BASE_DIR/conf/ $k8s_conf
    cp -r $K8S_BASE_DIR/yaml/ $k8s_conf
    cp -r $K8S_BASE_DIR/helm/hive/ $k8s_conf/helm/
}

function kill_k8s_am {
    waittime=$1
    echo "Sleep $waittime seconds at $(date)"
    sleep $waittime
    master=$(kubectl get pods -n hivemr3 | awk '{print $1}' | grep mr3master | head -n 1) > /dev/null 2>&1
    echo "Killing container $master at $(date)"
    kubectl delete pod $master -n hivemr3 > /dev/null 2>&1
}

function kill_k8s_workers {
    waittime=$1
    echo "Sleep $waittime seconds at $(date)"
    sleep $waittime
    workers=$(kubectl get pods -n hivemr3 | awk '{print $1}' | grep mr3worker | head -n 5) > /dev/null 2>&1
    for worker in $workers; do
        echo "Killing container $worker at $(date)"
        kubectl delete pod $worker -n hivemr3 > /dev/null 2>&1
    done
}

function kill_k8s_all_workers {
    waittime=$1
    echo "Sleep $waittime seconds at $(date)"
    sleep $waittime
    workers=$(kubectl get pods -n hivemr3 | awk '{print $1}' | grep mr3worker) > /dev/null 2>&1
    for worker in $workers; do
        echo "Killing container $worker at $(date)"
        kubectl delete pod $worker -n hivemr3 > /dev/null 2>&1
    done
}

function k8s_stop_cluster {
    echo -e "\n# Stopping Metastore and HiveServer2 "
    kubectl delete all --all -n hivemr3 > /dev/null
    kubectl -n hivemr3 delete statefulsets --all > /dev/null
    kubectl -n hivemr3 delete pod --all > /dev/null
    kubectl -n hivemr3 delete configmap --all > /dev/null
    kubectl -n hivemr3 delete svc --all > /dev/null
    kubectl -n hivemr3 delete secret --all > /dev/null
    kubectl -n hivemr3 delete role --all > /dev/null
    kubectl -n hivemr3 delete rolebinding --all > /dev/null
    kubectl -n hivemr3 delete deployments --all > /dev/null
    kubectl -n hivemr3 delete serviceaccount --all > /dev/null
    kubectl -n hivemr3 delete persistentvolumeclaims --all > /dev/null
    kubectl delete clusterrole node-reader > /dev/null
    kubectl delete persistentvolumes workdir-pv > /dev/null
    kubectl delete clusterrolebinding hive-clusterrole-binding > /dev/null
}
