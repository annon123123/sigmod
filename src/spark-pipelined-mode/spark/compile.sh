#!/bin/bash

set -e
export MAVEN_OPTS="-Xss64m -Xmx8g -XX:ReservedCodeCacheSize=2g"

function compile {
  if [[ $# -ge 1 ]]; then
    for proj in $@; do
      ./build/mvn -DskipTests -Pyarn -Phive -Phive-thriftserver -Pkubernetes -pl :spark-${proj}_2.12 clean install
    done
    ./build/mvn -DskipTests -Pyarn -Phive -Phive-thriftserver -Pkubernetes -pl :spark-assembly_2.12 clean install
  else
    ./build/mvn -DskipTests -Pyarn -Phive -Phive-thriftserver -Pkubernetes clean install
  fi
}

function install {
  mvn install:install-file \
    -Dfile=assembly/target/scala-2.12/jars/spark-core_2.12-3.2.2.jar \
    -DgroupId=org.apache.spark \
    -DartifactId=spark-core_2.12 \
    -Dversion=3.2.2.mr3.1.0 \
    -Dpackaging=jar

  cp assembly/target/scala-2.12/jars/spark-core_2.12-3.2.2.jar ~/.ivy2/cache/org.apache.spark/spark-core_2.12/jars/spark-core_2.12-3.2.2.jar
}

compile $@
install
