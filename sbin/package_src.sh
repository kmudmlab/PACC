#!/usr/bin/env bash
cd $(dirname $(readlink -f $0))
cd ..

tools/sbt assembly

tar zcvf pacc-0.1-src.tar.gz src/main/scala bin tools do_pacc_hadoop.sh do_pacc_spark.sh README.md LICENSE.txt simple.edge Makefile
