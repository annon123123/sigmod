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

function parse_args {
    START_THRIFTSERVER=false
    STOP_THRIFTSERVER=false
    REMAINING_ARGS=""
    while [[ -n $1 ]]; do
        case "$1" in
            start)
                START_THRIFTSERVER=true
                shift
                ;;
            stop)
                STOP_THRIFTSERVER=true
                shift
                ;;
            restart)
                START_THRIFTSERVER=true
                STOP_THRIFTSERVER=true
                shift
                ;;
            *)
                REMAINING_ARGS="$REMAINING_ARGS $1"
                shift
                ;;
        esac
    done
}

function thriftserver_service_init {
    common_setup_init
    hadoop_setup_init
    mr3_setup_init
    spark_setup_init
    spark_setup_init_conf $CONF_TYPE

    BASE_OUT=$SPARK_BASE_DIR/thriftserver-service-result
    spark_setup_init_output_dir $LOCAL_MODE $CONF_TYPE $BASE_OUT
}

function start_thriftserver {
    thriftserver_service_init

    declare log_dir="$OUT/spark-logs"

    spark_setup_config_spark_logs $log_dir
    spark_setup_init_run_configs $OUT $LOCAL_MODE $AM_PROCESS
    spark_setup_init_driver_opts
    spark_setup_extra_opts

    # Spark thirft server writes log file under SPARK_LOG_DIR
    # cf. sbin/spark-daemon.sh
    export SPARK_LOG_DIR=$log_dir

    # pass config from env.sh to conf/*/spark/hive-site.xml
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.database.host=$SPARK_DATABASE_HOST"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.database.name=$SPARK_DATABASE_NAME"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.secure.mode=$SECURE_MODE"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.hdfs.warehouse.dir=$SPARK_HDFS_WAREHOUSE"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.metastore.host=$SPARK_METASTORE_HOST"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.metastore.port=$SPARK_METASTORE_PORT"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.metastore.keytab.file=$HIVE_METASTORE_KERBEROS_PRINCIPAL"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.metastore.principal=$HIVE_METASTORE_KERBEROS_KEYTAB"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.server2.authentication.mode=$HIVE_SERVER2_AUTHENTICATION"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.server2.host=$SPARK_THRIFT_SERVER_HOST"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.server2.port=$SPARK_THRIFT_SERVER_PORT"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.metastore.kerberos.keytab.file=$HIVE_SERVER2_KERBEROS_KEYTAB"
    SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Dhive.metastore.kerberos.principal=$HIVE_SERVER2_KERBEROS_PRINCIPAL"

    # use '--master spark' because '--master mr3' is rejected from Spark 3.0.3
    $SPARK_HOME/sbin/start-thriftserver.sh \
        --driver-java-options "$SPARK_DRIVER_OPTS" \
        --master spark \
        --jars "$SPARK_DRIVER_JARS" \
        --driver-class-path "$SPARK_DRIVER_CP" \
        --conf spark.hadoop.yarn.timeline-service.enabled=false \
        $REMAINING_ARGS
}

function stop_thriftserver {
    $SPARK_HOME/sbin/stop-thriftserver.sh
}

function thriftserver_main {
    spark_setup_parse_args_common $@
    parse_args $REMAINING_ARGS

    if [ $STOP_THRIFTSERVER = true ]; then
        stop_thriftserver
    fi
    if [ $START_THRIFTSERVER = true ]; then
        start_thriftserver
    fi

}

thriftserver_main $@

