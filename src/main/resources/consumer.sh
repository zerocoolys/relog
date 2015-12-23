#!/usr/bin/env bash
mode=prod
bulk=1000
topic=huiyan_relog
consumerThreadNumber=8

BASE_DIR=`dirname $0`
JAVA_HOME=`${JAVA_HOME}`
DUMP_PATH=/root/dump

if [${JAVA_HOME} == ""]
    then
        JAVA_HOME=/usr/share/java /usr/local/java/jdk1.8.0_45
fi

while getopts "m:b:t:n:" arg
    do
        case $arg in
            "m")
                mode=$OPTARG
                ;;
            "b")
                bulk=$OPTARG
                ;;
            "t")
                topic=$OPTARG
                ;;
            "n")
                consumerThreadNumber=$OPTARG
                ;;
            "?")
                echo "unknow argument"
                ;;
        esac
    done

${JAVA_HOME}/bin/java -XX:+HeapDumpOnOutOfMemoryError  -XX:HeapDumpPath=${DUMP_PATH}  -Dfile.encoding=UTF-8 -Xms1g -Xmx1g -cp ${BASE_DIR}:${BASE_DIR}/lib/*:${BASE_DIR}/conf com.ss.main.RelogConsumerMain ${mode} ${topic} ${bulk} ${consumerThreadNumber}
