#!/usr/bin/env bash
cd $(dirname $(readlink -f $0))

hadoop fs -rm -r simple.edge
hadoop fs -put simple.edge simple.edge

hadoop jar pacc-0.1.jar cc.hadoop.PACC -Dmapred.reduce.tasks=1 -DnumPartitions=2 simple.edge simple.edge.cc

echo "The output files are in simple.edge.cc, and the content is as follows:"
hadoop fs -text simple.edge.cc/*
