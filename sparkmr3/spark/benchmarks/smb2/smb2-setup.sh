#!/bin/bash

SMB2_DIR=$(readlink -f $DIR/)
TPCDS_JAR=$SMB2_DIR/tpcds/target/scala-2.12/spark_mr3_tpcds_2.12-0.1.jar
TPCDS_CLASS="mr3.spark.tpcds.RunManyQueries"
QUERY_DIR="file://$BASE_DIR/spark/benchmarks/sparksql"
export SPARK_CONF_DIR=$BASE_DIR/conf/cluster/spark

function smb2_setup_init_output_dir {
  base_dir=$1

  mkdir -p $base_dir > /dev/null 2>&1
  SCRIPT_START_TIME=$(date +%s)
  declare time_stamp="$(common_setup_get_time $SCRIPT_START_TIME)"
  export OUT=$base_dir/smb2-$time_stamp-$(uuidgen | awk -F- '{print $1}')
  mkdir -p $OUT/smb2-log > /dev/null 2>&1
  echo -e "Output Directory: \n$OUT\n"

  common_setup_get_command > $OUT/command
  common_setup_get_git_info $MR3_SRC $OUT/mr3-dm-info
  common_setup_get_git_info $BASE_DIR $OUT/mr3-run-info
  common_setup_get_git_info $SPARK_MR3_SRC $OUT/spark-mr3-info

  OUT_CONF=$OUT/conf
  mkdir -p $OUT_CONF > /dev/null 2>&1

  cp $BASE_DIR/env.sh $OUT_CONF
  mr3_setup_update_conf_dir $OUT_CONF $CONF_TYPE
  common_setup_update_conf_dir $OUT_CONF $CONF_TYPE
  spark_setup_update_conf_dir $OUT_CONF

  yarn node -list 2> /dev/null | awk -F":" '{print $1}' | grep -v Node | sed 's/ //g' | sort -V > $OUT_CONF/slaves
}

function smb2_compile_tpcds {
  pushd $SMB2_DIR/tpcds > /dev/null
  sbt package
  popd > /dev/null
}

function smb2_run_yarn {
  jar_path=$1
  class=$2
  name=$3
  idx=$4
  shift 4

  SPARK_CONF="$SPARK_CONF --conf spark.hadoop.yarn.timeline-service.enabled=false --conf spark.yarn.populateHadoopClasspath=false --conf spark.ui.enabled=false"
  $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --name $name \
    --deploy-mode client \
    --class $class \
    $SPARK_CONF \
    $jar_path $@
}

function smb2_run_mr3 {
  jar_path=$1
  class=$2
  appid=$3
  idx=$4
  shift 4

  SPARK_CONF="$SPARK_CONF --conf spark.mr3.appid=$appid --conf spark.ui.enabled=false"
  $SPARK_BASE_DIR/run-spark-submit.sh \
    "--$CONF_TYPE" \
    "$SPARK_CONF" \
    --class $class \
    $jar_path $@
}

function smb2_run_mr3_not_share_am {
  jar_path=$1
  class=$2
  idx=$3
  shift 3

  SPARK_CONF="$SPARK_CONF --conf spark.ui.enabled=false"
  $SPARK_BASE_DIR/run-spark-submit.sh \
    "--$CONF_TYPE" \
    "$SPARK_CONF" \
    --class $class \
    $jar_path $@
}

function smb2_warm_up {
  SPARK_CONF="$SPARK_CONF --conf spark.mr3.keep.am=true"
  $SPARK_BASE_DIR/run-spark-submit.sh \
    "--$CONF_TYPE" \
    "$SPARK_CONF" \
    --class $TPCDS_CLASS \
    $TPCDS_JAR $DB_PATH $QUERY_DIR 0 1 0 false > $OUT/driver_warm_up.log 2>&1
  # TODO: Can we set 0 thread?

  APPID=`grep "MR3 Application ID" $OUT/driver_warm_up.log | awk -F":" '{print $NF}' | head -n 1 | sed 's/ //g'`
  echo "MR3 AppMaster ID: $APPID"
}

function smb2_stop_am {
  $BASE_DIR/mr3/master-control.sh closeDagAppMaster $APPID
}
