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
source $TEZ_BASE_DIR/tez-setup.sh

function compile_tez_print_usage {
    echo "Usage: compile-tez.sh"
    echo " -h/--help                              Print the usage."
    tez_setup_print_usage_tezsrc
    echo " <mvn option>                           Add a Maven option; may be repeated at the end."
    echo ""
    echo "Example: ./compile-tez.sh"
    echo ""
}

function compile_tez_parse_args {
     while [[ -n $1 ]]; do
        case "$1" in
            -h|--help)
                compile_tez_print_usage
                exit 0
                ;;
            *)
                TEZ_MVN_OPTS=$@
                break
                ;;
        esac
    done
}

function compile_tez_main {
    tez_setup_parse_args_common $@
    compile_tez_parse_args $REMAINING_ARGS

    # setup environment variables, e.g. PATH
    common_setup_init
    mr3_setup_init
    tez_setup_init

    echo -e "\n# Compiling Tez-MR3 ($(basename $TEZ_SRC), $TEZ_REV) #" >&2

    COMPILE_OUT_FILE=$LOG_BASE_DIR/compile-tez.log
    rm -rf $COMPILE_OUT_FILE

    # install mr3-core jar to local repo
    MR3_CORE_JAR_PATH=$MR3_JARS/$MR3_CORE_JAR
    mvn install:install-file -Dfile=$MR3_CORE_JAR_PATH -DgroupId=mr3 -DartifactId=mr3-core -Dversion=$MR3_REV -Dpackaging=jar 2>&1 | tee -a $COMPILE_OUT_FILE
    if [ $? -ne 0 ]; then
        echo "\nFailed to install $MR3_CORE_JAR to local maven repo.\n"
        exit 1
    fi

    # build and create jars in tez-dist, then install them to local repo
    cd $TEZ_SRC
    mvn clean install -DskipTests=true -Dmaven.javadoc.skip=true $TEZ_MVN_OPTS 2>&1 | tee -a $COMPILE_OUT_FILE
    echo -e "\nCommand: mvn clean install -DskipTests=true -Dmaven.javadoc.skip=true $TEZ_MVN_OPTS \n" 2>&1 | tee -a $COMPILE_OUT_FILE

    if [[ $(grep -c "BUILD FAILURE" $COMPILE_OUT_FILE) -gt 0 ]]; then
        cat $COMPILE_OUT_FILE >&2
        echo -e "\nCompilation failed" >&2
        exit 1
    fi

    # copy to mr3-run
    rm -rf $TEZ_LIB_DIR
    mkdir -p $TEZ_LIB_DIR
    if [[ $TEZ_USE_MINIMAL = true ]]; then
      TEZ_DIST=$TEZ_SRC/tez-dist/target/tez-${TEZ_REV}-minimal
    else
      TEZ_DIST=$TEZ_SRC/tez-dist/target/tez-${TEZ_REV}
    fi
    cp -r $TEZ_DIST/* $TEZ_LIB_DIR
    
    echo -e "Output: $TEZ_LIB_DIR\n"    
    ls -R $TEZ_LIB_DIR

    $TEZ_BASE_DIR/upload-hdfslib-tez.sh 

    echo -e "\nCompilation succeeded" >&2
}

compile_tez_main $@
