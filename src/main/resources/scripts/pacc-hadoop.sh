#!/usr/bin/env bash

graphs="sk.tsv pt.tsv lj.tsv fs.tsv twt.tsv sd.tsv yw.tsv"

for graph in $graphs; do

hadoop jar pacc-ext-0.1.jar cc.hadoop.PACCLocalOpt -Dmapred.reduce.tasks=120 -DnumPartitions=120 -DlocalThreshold=20000000 hmpark/graphs/$graph hmpark/pacc-res/$graph
hadoop jar pacc-ext-0.1.jar cc.hadoop.PACCOpt -Dmapred.reduce.tasks=120 -DnumPartitions=120 -DlocalThreshold=20000000 hmpark/graphs/$graph hmpark/pacc-res/$graph
hadoop jar pacc-ext-0.1.jar cc.hadoop.PACC -Dmapred.reduce.tasks=120 -DnumPartitions=120 hmpark/graphs/$graph hmpark/pacc-res/$graph
hadoop jar pacc-ext-0.1.jar cc.hadoop.AltOpt -Dmapred.reduce.tasks=120 -DnumPartitions=120 hmpark/graphs/$graph hmpark/pacc-res/$graph
hadoop jar pacc-ext-0.1.jar cc.hadoop.Alt -Dmapred.reduce.tasks=120 hmpark/graphs/$graph hmpark/pacc-res/$graph

done