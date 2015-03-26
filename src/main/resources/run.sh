#!/bin/bash
host=127.0.0.1
cluster_name=elasticsearch
bulk=1000

while getopts "h:c:b:" arg
    do
        case $arg in
            "h")
                host=$OPTARG
                ;;
            "c")
                cluster_name=$OPTARG
                ;;
            "b")
                bulk=$OPTARG
                ;;
            "?")
                echo "unknow argument"
                ;;
        esac
    done

java -cp .:./lib/*:./conf com.ss.main.Relog ${host} ${cluster_name} ${bulk}