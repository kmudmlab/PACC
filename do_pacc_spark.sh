#!/usr/bin/env bash
cd $(dirname $(readlink -f $0))

hadoop fs -rm -r simple.edge
hadoop fs -put simple.edge simple.edge

spark-submit --master yarn \
             --deploy-mode client \
             --executor-cores 1 --executor-memory 6g \
             --class cc.spark.PACC \
             pacc-ext-0.1.jar -t 1000 -p 2 hdfs:///user/$USER/simple.edge hdfs:///user/$USER/simple.edge.cc

echo "The output files are in simple.edge.cc, and the content is as follows:"
hadoop fs -text simple.edge.cc/*
