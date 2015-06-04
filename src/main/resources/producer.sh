#!/usr/bin/env bash
mode=prod
topic=relog
port=20001

while getopts "m:p:t:" arg
    do
        case $arg in
            "m")
                mode=$OPTARG
                ;;
            "p")
                port=$OPTARG
                ;;
            "t")
                topic=$OPTARG
                ;;
            "?")
                echo "unknow argument"
                ;;
        esac
    done

java -Xms2g -Xmx2g -cp .:./lib/*:./conf com.ss.main.RelogProducerMain ${mode} ${topic} ${port}