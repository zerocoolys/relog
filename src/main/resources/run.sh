#!/bin/bash
mode=dev
port=28888
bulk=1000

while getopts "m:p:b:" arg
    do
        case $arg in
            "m")
                mode=$OPTARG
                ;;
            "p")
                port=$OPTARG
                ;;
            "b")
                bulk=$OPTARG
                ;;
            "?")
                echo "unknow argument"
                ;;
        esac
    done

java -cp .:./lib/*:./conf com.ss.main.Relog ${mode} ${port} ${bulk}