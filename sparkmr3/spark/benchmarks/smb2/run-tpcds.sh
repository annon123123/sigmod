#!/bin/bash

DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR=$(readlink -f $DIR/../../..)
source $BASE_DIR/env.sh
source $BASE_DIR/common-setup.sh
source $MR3_BASE_DIR/mr3-setup.sh
source $SPARK_BASE_DIR/spark-setup.sh
source $DIR/smb2-setup.sh

function print_usage {
  echo "Usage: run-tpcds.sh --mode <Mode> [Option(s)]"
  echo ""
  echo "Mode: mr3, yarn, mr3_not_share_am"
  echo ""
  echo "Option list"
  echo "  --local            Use configuration files in conf/local/."
  echo "  --cluster          Use configuration files in conf/cluster/ (default)."
  echo "  --tpcds            Use configuration files in conf/tpcds/."
  echo "  --driver <n>       Set the number of driver to run concurrently."
  echo "  --thread <n>       Set the number of thread in each driver to submit queries concurrently."
  echo "  --repeat <n>       Repeat the query sequence <n> times."
  echo "  --delay <n>        Set a delay between each driver."
  echo "  --random           Run the query sequence in random order."
  echo "  --db_path <path>   Set the path of TPC-DS text data (default: hdfs:///tmp/tpcds-generate/100)."
  echo "  --query_dir <path> Set the path of the directory of TPC-DS query files (default: file://\$BASE_DIR/spark/benchmarks/sparksql)."
  echo "  --query <seq>      Set a query sequence to run (default: 1 to 99). Must be a comma separated list. This option ignores --querystart and --queryend."
  echo "  --querystart <idx> Set the first index of the query sequence (default: 1)."
  echo "  --queryend <idx>   Set the last index of the query sequence (default: 99)."
  echo "  --conf <k>=<v>     Add a Spark configuration key/value; may be repeated at the end."
  echo "  -v, --verbose      Print the result with more details."
}

function parse_args {
  MODE=""

  CONF_TYPE=cluster
  LOCAL_MODE=false

  NUM_DIRVERS=1
  NUM_THREADS=1
  REPEAT=1
  DELAY=0
  USE_RANDOM=false

  DB_PATH="hdfs:///tmp/tpcds-generate/100"

  QUERY_SEQ=""
  QUERY_START=1
  QUERY_END=99

  SPARK_CONF=""

  VERBOSE_PRINT=false

  while [[ -n $1 ]]; do
    case "$1" in
      --mode)
        MODE=$2
        shift 2
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
      --driver)
        NUM_DIRVERS=$2
        shift 2
        ;;
      --thread)
        NUM_THREADS=$2
        shift 2
        ;;
      --repeat)
        REPEAT=$2
        shift 2
        ;;
      --delay)
        DELAY=$2
        shift 2
        ;;
      --random)
        USE_RANDOM=true
        shift
        ;;
      --db_path)
        DB_PATH=$2
        shift 2
        ;;
      --query_dir)
        QUERY_DIR=$2
        shift 2
        ;;
      --query)
        QUERY_SEQ=$2
        shift 2
        ;;
      --querystart)
        QUERY_START=$2
        shift 2
        ;;
      --queryend)
        QUERY_END=$2
        shift 2
        ;;
      --conf)
        SPARK_CONF="$SPARK_CONF --conf $2"
        shift 2
        ;;
      -v|--verbose)
        VERBOSE_PRINT=true
        shift
        ;;
      *)
        shift
        ;;
    esac
  done

  if [[ $MODE != "mr3" ]] && [[ $MODE != "yarn" ]] && [[ $MODE != "mr3_not_share_am" ]]; then
    print_usage
    exit 1
  fi
}

function run_mr3 {
  if [[ $NUM_DIRVERS -gt 1 ]]; then
    smb2_warm_up

    COUNTER=0
    while [[ $COUNTER -lt $NUM_DIRVERS ]]; do
      smb2_run_mr3 $TPCDS_JAR $TPCDS_CLASS $APPID $COUNTER $DB_PATH $QUERY_DIR $QUERY_SEQ $NUM_THREADS $REPEAT $USE_RANDOM > $LOG_OUT_DIR/driver_${COUNTER}.log 2>&1 &
      let COUNTER=COUNTER+1
      sleep $DELAY
    done

    wait

    smb2_stop_am
  else
    smb2_run_mr3_not_share_am $TPCDS_JAR $TPCDS_CLASS 0 $DB_PATH $QUERY_DIR $QUERY_SEQ $NUM_THREADS $REPEAT $USE_RANDOM > $LOG_OUT_DIR/driver_0.log 2>&1 &

    wait
  fi
}

function run_mr3_not_share_am {
  COUNTER=0
  while [[ $COUNTER -lt $NUM_DIRVERS ]]; do
    smb2_run_mr3_not_share_am $TPCDS_JAR $TPCDS_CLASS $COUNTER $DB_PATH $QUERY_DIR $QUERY_SEQ $NUM_THREADS $REPEAT $USE_RANDOM > $LOG_OUT_DIR/driver_${COUNTER}.log 2>&1 &
    let COUNTER=COUNTER+1
    sleep $DELAY
  done

  wait
}

function run_yarn {
  COUNTER=0
  while [[ $COUNTER -lt $NUM_DIRVERS ]]; do
    smb2_run_yarn $TPCDS_JAR $TPCDS_CLASS "TPCDS-$COUNTER" $COUNTER $DB_PATH $QUERY_DIR $QUERY_SEQ $NUM_THREADS $REPEAT $USE_RANDOM > $LOG_OUT_DIR/driver_${COUNTER}.log 2>&1 &
    let COUNTER=COUNTER+1
    sleep $DELAY
  done

  wait
}

function set_query_seq {
  QUERY_ALL=",1,2,3,4,5,6,7,8,9,10,11,12,13,14-1,14-2,15,16,17,18,19,20,21,22,23-1,23-2,24-1,24-2,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39-1,39-2,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,"

  echo $QUERY_ALL \
    | awk -F",$QUERY_START," -v start="$QUERY_START" '{print start","$2}' \
    | awk -F",$QUERY_END," -v end="$QUERY_END" '{print $1","end}' > $OUT/queries.tmp

  cat $OUT/queries.tmp | sed 's/^,//' | sed 's/,$//' > $OUT/queries

  rm $OUT/queries.tmp

  QUERY_SEQ=`cat $OUT/queries | head -n 1`
}

function print_result {
  COUNTER=0
  while [[ $COUNTER -lt $NUM_DIRVERS ]]; do
    log_file="$LOG_OUT_DIR/driver_${COUNTER}.log"
    let COUNTER=COUNTER+1

    echo "===== Driver $COUNTER ====="

    if [[ $VERBOSE_PRINT == true ]]; then
      grep Iter $log_file | grep Query
    else
      grep Iter $log_file | grep Query | awk '{print $8/1000}'
    fi

    echo ""
  done
}

function main {
  parse_args $@

  smb2_setup_init_output_dir $DIR/tpcds-result

  if [[ -z $QUERY_SEQ ]]; then
    set_query_seq
  else
    echo $QUERY_SEQ > $OUT/queries
  fi

  if [[ ! -f $TPCDS_JAR ]]; then
    smb2_compile_tpcds
  fi

  common_setup_sync_time false $OUT_CONF/slaves

  LOG_OUT_DIR=$OUT/smb2-log

  if [[ $MODE == "mr3" ]]; then
    run_mr3
  elif [[ $MODE == "mr3_not_share_am" ]]; then
    run_mr3_not_share_am
  elif [[ $MODE == "yarn" ]]; then
    run_yarn
  fi

  print_result

  date
}

main $@
