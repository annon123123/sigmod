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

# get current dir of this script and mr3-run dir
DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR=$(readlink -f $DIR/..)
source $BASE_DIR/env.sh
source $BASE_DIR/common-setup.sh
source $MR3_BASE_DIR/mr3-setup.sh
source $SPARK_BASE_DIR/spark-setup.sh

function compile_spark_print_usage {
    echo "Usage: compile-spark.sh"
    echo " -h/--help                              Print the usage."
    echo ""
    echo "Example: ./compile-spark.sh"
    echo ""
}

function compile_spark_parse_args {
     while [[ -n $1 ]]; do
        case "$1" in
            -h|--help)
                compile_spark_print_usage
                exit 0
                ;;
            *)
                REMAINING=$@
                break
                ;;
        esac
    done
}

function compile_spark_main {
    spark_setup_parse_args_common $@
    compile_spark_parse_args $REMAINING_ARGS

    # setup environment variables, e.g. PATH
    common_setup_init
    mr3_setup_init
    spark_setup_init

    echo -e "\n# Compiling Spark-MR3 ($(basename $SPARK_MR3_SRC), $SPARK_MR3_REV) #" >&2

    COMPILE_OUT_FILE=$LOG_BASE_DIR/compile-spark.log
    rm -rf $COMPILE_OUT_FILE

    # install mr3-core jar to local repo
    MR3_CORE_JAR_PATH=$MR3_JARS/$MR3_CORE_JAR
    mvn install:install-file -Dfile=$MR3_CORE_JAR_PATH -DgroupId=mr3 -DartifactId=mr3-core -Dversion=$MR3_REV -Dpackaging=jar 2>&1 | tee -a $COMPILE_OUT_FILE
    if [ $? -ne 0 ]; then
        echo "\nFailed to install $MR3_CORE_JAR to local maven repo.\n"
        exit 1
    fi

    # build and create jars in spark-dist, then install them to local repo
    pushd $SPARK_MR3_SRC > /dev/null

    # clean up existing jar files to ensure that maven creates a new jar file
    mvn clean

    # cmd="sbt \"; package; assembly\""
    echo -e "\nsbt \"package; assembly\"\n" 2>&1 | tee -a $COMPILE_OUT_FILE
    sbt "; package; assembly" 2>&1 | tee -a $COMPILE_OUT_FILE

    if [[ $(grep -c "BUILD FAILURE" $COMPILE_OUT_FILE) -gt 0 ]]; then
        cat $COMPILE_OUT_FILE >&2
        echo -e "\nCompilation failed" >&2
        exit 1
    fi

    SBT_JAR=$SPARK_MR3_SRC/target/scala-2.12/$SPARK_MR3_ASSEMBLY_JAR
    MVN_JAR=$SPARK_MR3_SRC/target/spark-mr3-assembly-$SPARK_MR3_REV.jar
    mvn install:install-file -Dfile=$SBT_JAR -DgroupId=org.apache.spark -DartifactId=spark-mr3_2.12 -Dversion=$SPARK_MR3_REV -Dpackaging=jar
    mvn package
    cp -f $MVN_JAR $SBT_JAR

    popd > /dev/null

    rm -rf $SPARK_MR3_LIB_DIR
    mkdir -p $SPARK_MR3_LIB_DIR
    cp -r $SBT_JAR $SPARK_MR3_LIB_DIR

    echo -e "Output: $SPARK_LIB_BASE_DIR\n"
    ls -R $SPARK_LIB_BASE_DIR

    $SPARK_BASE_DIR/upload-hdfslib-spark.sh

    echo -e "\nCompilation succeeded" >&2
}

compile_spark_main $@
