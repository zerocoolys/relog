#!/usr/bin/env bash
mode=prod
bulk=1000
topic=relog
consumerThreadNumber=4

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

java -Xms2g -Xmx2g -cp .:./lib/*:./conf com.ss.main.RelogConsumerMain ${mode} ${topic} ${bulk} ${consumerThreadNumber}