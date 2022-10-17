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

function mr3_setup_print_usage_amprocess {
    echo " --amprocess                            Run the MR3 DAGAppMaster in LocalProcess mode."
}

function mr3_setup_parse_args_common {
    # LOCAL_MODE indicates if --local is used.
    #   - if LOCAL_MODE=true, AM_MODE is set to "local-thread" by default.
    #   - if LOCAL_MODE=false, make sure that Yarn containers (for either DAGAppMaster or ContainerWorker) are created.
    #   Invariant: We cannot have both LOCAL_MODE=true (--local) and AM_MODE="yarn".
    #   However, We can have LOCAL_MODE=false (--cluster) and AM_MODE="local-thread"/"local-process".
    # AM_MODE can be set again in run_mr3_parse_args().
    LOCAL_MODE=true
    CONF_TYPE=local
    # do not remove LOCAL_MODE which is used to update HADOOP_HOME 

    while [[ -n $1 ]]; do
        case "$1" in
            --local)
                LOCAL_MODE=true
                CONF_TYPE=local
                AM_MODE=${AM_MODE:-"local-thread"}
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

function mr3_setup_init {
    MR3_SRC_REV=$(common_setup_get_git_rev $MR3_SRC)

    MR3_LIB_DIR=$MR3_BASE_DIR/mr3lib
    MR3_JARS=$MR3_BASE_DIR/mr3jar
    MR3_UI=$MR3_BASE_DIR/mr3-ui
    MR3_CLASSPATH=$MR3_JARS/*:$MR3_LIB_DIR/*

    MR3_HDFS_LIB_DIR=$HDFS_LIB_DIR/mr3

    MR3_CORE_JAR=mr3-core-$MR3_REV.jar
    MR3_CORE_TEST_JAR=mr3-core-$MR3_REV-tests.jar
    MR3_TEZ_JAR=mr3-tez-$MR3_REV.jar
    MR3_TEZ_TEST_JAR=mr3-tez-$MR3_REV-tests.jar
    MR3_TEZ_EXAMPLE_JAR=mr3-tez-examples-$MR3_REV.jar
    MR3_SPARK_JAR=mr3-spark-$MR3_REV.jar

    MR3_TEZ_ASSEMBLY_JAR=mr3-tez-$MR3_REV-assembly.jar
    MR3_SPARK_ASSEMBLY_JAR=mr3-spark-$MR3_REV-assembly.jar

    # okay to use even when using LOG4J version 1 because it uses -Dlog4j.configuration, not -Dlog4j.configurationFile
    export YARN_CLIENT_OPTS="$YARN_CLIENT_OPTS -Dlog4j.configurationFile=mr3-container-log4j2.properties"
}

function mr3_setup_update_conf_dir {
    conf_dir=$1
    conf_type=$2

    MR3_CONF_DIR=$BASE_DIR/conf/$conf_type/mr3

    cp -r $MR3_CONF_DIR/* $conf_dir
}

function mr3_setup_check_compile {
    local_mode=$1

    is_compile_local=$(ls -q1 $MR3_JARS | wc -l)
    if [[ $is_compile_local = "0" ]]; then
        printf "\n# Please compile mr3 ($MR3_JARS is empty) #\n\n"
        exit 1
    fi

    if [[ $local_mode = false ]]; then
        is_compile_cluster=$(hdfs dfs -ls $MR3_HDFS_LIB_DIR 2> /dev/null | wc -l)
        if [[ $is_compile_cluster = "0" ]]; then
            printf "\n# Please compile mr3 ($MR3_HDFS_LIB_DIR in hdfs is empty) #\n\n"
            exit 1
        fi
    fi
}

function mr3_setup_update_yarn_opts {
    local_mode=$1
    log_dir=$2
    am_mode=$3
    use_assembly=$4

    if [[ $local_mode = false ]]; then
        if [[ $LOCALIZE = false ]]; then
            MR3_LIBURIS_OPTS="-Dliburis="
            MR3_AUXURIS_OPTS="-Dauxuris="
            MR3_ADD_CLASSPATH_OPTS="-Daddclasspath=$REMOTE_BASE_DIR/mr3/mr3jar/*:$REMOTE_BASE_DIR/mr3/mr3lib/*"
            if [[ $MR3_TEZ_ENABLED = true ]]; then
                MR3_ADD_CLASSPATH_OPTS="$MR3_ADD_CLASSPATH_OPTS:$REMOTE_BASE_DIR/tez/tezjar/tez-$TEZ_REV/*"
            fi
            if [[ $MR3_SPARK_ENABLED = true ]]; then
                MR3_ADD_CLASSPATH_OPTS="$MR3_ADD_CLASSPATH_OPTS:$REMOTE_BASE_DIR/spark/sparkjar/*"
            fi
        else
            MR3_LIBURIS_OPTS="-Dliburis="
            if [[ $MR3_TEZ_ENABLED = true ]]; then
                MR3_LIBURIS_OPTS="-Dliburis=$TEZ_HDFS_LIB_DIR/tar/tez-$TEZ_REV.tar.gz"
            fi
            if [[ $MR3_SPARK_ENABLED = true ]]; then
                # MR3 needs SPARK_TAR, e.g., because of Hadoop jar files
                MR3_LIBURIS_OPTS="-Dliburis=$SPARK_HDFS_LIB_DIR/tar/$SPARK_TAR"
            fi

            MR3_AUXURIS_OPTS="-Dauxuris="
            if [[ $use_assembly = true ]]; then
                if [[ $MR3_TEZ_ENABLED = true ]]; then
                    MR3_AUXURIS_OPTS="-Dauxuris=$MR3_HDFS_LIB_DIR/$MR3_TEZ_ASSEMBLY_JAR"
                fi
                if [[ $MR3_SPARK_ENABLED = true ]]; then
                    MR3_AUXURIS_OPTS="-Dauxuris=$MR3_HDFS_LIB_DIR/$MR3_SPARK_ASSEMBLY_JAR"
                fi
            else
              MR3_AUXURIS_OPTS="-Dauxuris=$MR3_HDFS_LIB_DIR"
            fi

            MR3_ADD_CLASSPATH_OPTS="-Daddclasspath="
        fi
        export YARN_OPTS="$YARN_OPTS $MR3_LIBURIS_OPTS $MR3_AUXURIS_OPTS $MR3_ADD_CLASSPATH_OPTS"

        # token renewal
        if [[ $TOKEN_RENEWAL_HDFS_ENABLED = "true" ]]; then
            MR3_TOKEN_OPTS="-Dmr3.principal=$USER_PRINCIPAL -Dmr3.keytab=$USER_KEYTAB -Dmr3.token.renewal.hdfs.enabled=true"
            if [[ $TOKEN_RENEWAL_HIVE_ENABLED = "true" ]]; then
                MR3_TOKEN_OPTS="$MR3_TOKEN_OPTS -Dmr3.token.renewal.hive.enabled=true"
            fi
        fi
        export YARN_OPTS="$YARN_OPTS $MR3_TOKEN_OPTS"
    fi

    export YARN_OPTS="$YARN_OPTS -Dmr3.am.heapsize=$MR3_AM_HEAPSIZE"

    # -Dmr3.XXX=YYY directly adds (mr3.XXX -> YYY) to MR3Conf
    MR3_LOG_OPTS="-Dmr3.am.local.log-dir=$log_dir -Dyarn.app.container.log.dir=$log_dir -Dmr3.am.log.level=$LOG_LEVEL -Dmr3.container.log.level=$LOG_LEVEL"
    export YARN_OPTS="$YARN_OPTS $MR3_LOG_OPTS"

    # AMProcesssMode
    if [[ $am_mode = "local-process" ]]; then
      if [[ $use_assembly = true ]]; then
        MR3_AM_MODE_CLASSPATH="-Dmr3.master.mode=local-process -Damprocess.classpath=$SPARK_JARS_DIR/*:$MR3_LIB_DIR/$MR3_SPARK_ASSEMBLY_JAR:$MR3_LIB_DIR/$MR3_TEZ_ASSEMBLY_JAR:$TEZ_LIB_DIR/*:$TEZ_LIB_DIR/lib/*"
      else
        MR3_AM_MODE_CLASSPATH="-Dmr3.master.mode=local-process -Damprocess.classpath=$SPARK_JARS_DIR/*:$MR3_JARS/*:$MR3_LIB_DIR/*:$TEZ_LIB_DIR/*:$TEZ_LIB_DIR/lib/*"
      fi
    else
        MR3_AM_MODE_CLASSPATH="-Damprocess.classpath="
    fi
    export YARN_OPTS="$YARN_OPTS $MR3_AM_MODE_CLASSPATH"

    # TEZ_USE_MINIMAL
    export YARN_OPTS="$YARN_OPTS -Dtez.use.minimal=$TEZ_USE_MINIMAL"

    # profile
    MR3_HONEST_OPTS="-Dam.honestopts= -Dcontainer.honestopts="
    MR3_JFR_OPTS="-Dam.jfropts1= -Dam.jfropts2= -Dam.jfropts3= -Dam.jfropts4= -Dam.jfropts5= -Dam.jfropts6= -Dcontainer.jfropts1= -Dcontainer.jfropts2= -Dcontainer.jfropts3= -Dcontainer.jfropts4= -Dcontainer.jfropts5= -Dcontainer.jfropts6="
    MR3_YOURKIT_OPTS="-Dam.yourkitopts= -Dcontainer.yourkitopts="
    if [[ $PROFILE = "true" ]]; then
        if [[ $HONEST_PROFILER = "true" ]]; then
            case "$PROFILE_MODE" in
                "master")
                    common_setup_get_profiler_options $PROFILE_DIR_MASTER
                    MR3_HONEST_OPTS="$MR3_HONEST_OPTS -Dam.honestopts=$HONEST_OPTS"
                    ;;
                "workers")
                    common_setup_get_profiler_options $PROFILE_DIR_WORKER
                    MR3_HONEST_OPTS="$MR3_HONEST_OPTS -Dcontainer.honestopts=$HONEST_OPTS"
                    ;;
                "all")
                    common_setup_get_profiler_options $PROFILE_DIR_MASTER
                    MR3_HONEST_OPTS="$MR3_HONEST_OPTS -Dam.honestopts=$HONEST_OPTS"

                    common_setup_get_profiler_options $PROFILE_DIR_WORKER
                    MR3_HONEST_OPTS="$MR3_HONEST_OPTS -Dcontainer.honestopts=$HONEST_OPTS"
                    ;;
                *)
                    echo "Unknown $PROFILE_MODE"
                    ;;
            esac
        fi
        if [[ $JAVA_FLIGHT_RECORDER = "true" ]]; then
            case "$PROFILE_MODE" in
                "master")
                    common_setup_get_profiler_options $PROFILE_DIR_MASTER
                    opt_idx=0
                    for opt in $JFR_OPTS; do
                        opt_idx=$((opt_idx+1))
                        MR3_JFR_OPTS="$MR3_JFR_OPTS -Dam.jfropts$opt_idx=$opt"
                    done
                    ;;
                "workers")
                    common_setup_get_profiler_options $PROFILE_DIR_WORKER
                    opt_idx=0
                    for opt in $JFR_OPTS; do
                        opt_idx=$((opt_idx+1))
                        MR3_JFR_OPTS="$MR3_JFR_OPTS -Dcontainer.jfropts$opt_idx=$opt"
                    done
                    ;;
                "all")
                    common_setup_get_profiler_options $PROFILE_DIR_MASTER
                    opt_idx=0
                    for opt in $JFR_OPTS; do
                        opt_idx=$((opt_idx+1))
                        MR3_JFR_OPTS="$MR3_JFR_OPTS -Dam.jfropts$opt_idx=$opt"
                    done

                    common_setup_get_profiler_options $PROFILE_DIR_WORKER
                    opt_idx=0
                    for opt in $JFR_OPTS; do
                        opt_idx=$((opt_idx+1))
                        MR3_JFR_OPTS="$MR3_JFR_OPTS -Dcontainer.jfropts$opt_idx=$opt"
                    done
                    ;;
                *)
                    echo "Unknown $PROFILE_MODE"
                    ;;
            esac
        fi
        if [[ $YOURKIT = "true" ]]; then
            case "$PROFILE_MODE" in
                "master")
                    common_setup_get_profiler_options $PROFILE_DIR_MASTER
                    MR3_YOURKIT_OPTS="$MR3_YOURKIT_OPTS -Dam.yourkitopts=$YOURKIT_OPTS"
                    ;;
                "workers")
                    common_setup_get_profiler_options $PROFILE_DIR_WORKER
                    MR3_YOURKIT_OPTS="$MR3_YOURKIT_OPTS -Dcontainer.yourkitopts=$YOURKIT_OPTS"
                    ;;
                "all")
                    common_setup_get_profiler_options $PROFILE_DIR_MASTER
                    MR3_YOURKIT_OPTS="$MR3_YOURKIT_OPTS -Dam.yourkitopts=$YOURKIT_OPTS"

                    common_setup_get_profiler_options $PROFILE_DIR_WORKER
                    MR3_YOURKIT_OPTS="$MR3_YOURKIT_OPTS -Dcontainer.yourkitopts=$YOURKIT_OPTS"
                    ;;
                *)
                    echo "Unknown $PROFILE_MODE"
                    ;;
            esac
        fi
    fi
    export YARN_OPTS="$YARN_OPTS $MR3_HONEST_OPTS $MR3_JFR_OPTS $MR3_YOURKIT_OPTS"

    export YARN_OPTS="$YARN_OPTS $CONF_OPTS"
}

function mr3_setup_run_core {
    declare job_args=$@

    hadoop_setup_run_yarn $MR3_JARS/$MR3_CORE_JAR $job_args
}

function mr3_setup_run_tez_examples {
    declare job_args=$@

    hadoop_setup_run_yarn $MR3_JARS/$MR3_TEZ_EXAMPLE_JAR $job_args
}

function mr3_setup_run_asm_examples {
    declare job_args=$@

    hadoop_setup_run_yarn $MR3_JARS/$MR3_ASM_EXAMPLE_JAR $job_args
}

#
# for getting mr3 logs
#

function mr3_setup_get_report {
    out_dir=$1
    client_log_file=$2
    log_dir=$3
    stat_file=$4
    local_mode=$5

    if [[ $AM_MODE = "yarn" ]] || [[ $local_mode = false ]]; then
        declare YARN_APPLICATION_ID
        hadoop_setup_get_yarn_app_id_from_line "$(grep application_ $client_log_file | head -n1)"
        if [ $? -ne 0 ]; then
            printf "\n# ERROR: Failed to get appid from $client_log_file #\n\n"
            return 1
        fi
        appid=$YARN_APPLICATION_ID

        mkdir -p $log_dir/$appid
        hadoop_setup_get_yarn_report $appid $out_dir $log_dir/$appid $stat_file
    fi
}
