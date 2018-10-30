#!/usr/bin/env bash
cd $(dirname $(readlink -f $0))
cd ..

tools/sbt assembly

tar zcvf pacc-0.1.tar.gz bin tools do_pacc_hadoop.sh README.md LICENSE.txt simple.edge Makefile
