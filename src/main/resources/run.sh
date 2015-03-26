#!/bin/bash
host=127.0.0.1
port=9300
cluster_name=elasticsearch
bulk=1000

while getopts "h:p:c:b:" arg
    do
        case $arg in
            "h")
                host=$OPTARG
                ;;
            "p")
                port=$OPTARG
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

java -cp .:./lib/*:./conf com.ss.main.Relog ${host} ${port} ${cluster_name} ${bulk}