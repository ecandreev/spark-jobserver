#!/bin/bash
# Script to start the job manager
# args: <driver mode> <work dir for context> <context config> <akka master> [--proxy_user <proxy user>] [--master <spark masters>] [--supervise <on>]
set -e

DRIVER_MODE=$1
WORKING_DIR=$2
CONTEXT_CONF=$3
AKKA_MASTER_ADDRESS=$4

while [[ $# -gt 4 ]] ;do
  case "$5" in
    --proxy_user)
      SPARK_PROXY_USER_PARAM="$6"
      shift
    ;;
    --master)
      SPARK_MASTERS="$6"
      shift
    ;;
    --supervise)
      SPARK_SUPERVISE="$6"
      shift
    ;;
  esac
  shift
done

get_abs_script_path() {
   pushd . >/dev/null
   cd $(dirname $0)
   appdir=$(pwd)
   popd  >/dev/null
}
get_abs_script_path

. $appdir/setenv.sh

# Override logging options to provide per-context logging
LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties
              -DLOG_DIR=$WORKING_DIR"

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

MAIN="spark.jobserver.JobManager"

DRIVER_MODE_OPTS=""
if [ $DRIVER_MODE == "mesos-cluster" ]; then
    DRIVER_MODE_OPTS="--master $MESOS_SPARK_DISPATCHER --deploy-mode cluster"
    appdir=$REMOTE_JOBSERVER_DIR
elif [ $DRIVER_MODE == "standalone-cluster" ]; then
  DRIVER_MODE_OPTS="--master $SPARK_MASTERS --deploy-mode cluster"
fi

if [ $SPARK_SUPERVISE == "on" ]; then
  DRIVER_MODE_OPTS="$DRIVER_MODE_OPTS --supervise"
fi

if [ ! -z $SPARK_PROXY_USER_PARAM ]; then
  cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  --proxy-user $SPARK_PROXY_USER_PARAM
  $DRIVER_MODE_OPTS
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES"
  $appdir/spark-job-server.jar $WORKING_DIR $CONTEXT_CONF $AKKA_MASTER_ADDRESS $conffile'
else
  cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES"
  $DRIVER_MODE_OPTS
  $appdir/spark-job-server.jar $WORKING_DIR $CONTEXT_CONF $AKKA_MASTER_ADDRESS $conffile'
fi

eval $cmd > /dev/null 2>&1 &
# exec java -cp $CLASSPATH $GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $MAIN $1 $2 $conffile 2>&1 &
