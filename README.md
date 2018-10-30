# PACC

- PACC (Partition Aware Connected Components) is a tool for computing connected components in a large graph.
- Given an undirected simple graph, PACC returns the minimum reachable node id for each node.
- It runs in parallel, distributed manner on top of Hadoop and Spark, which are widely-used distributed computation system.



## Build

PACC uses [SBT (Simple Build Tool)](http://www.scala-sbt.org/) to manage dependencies and build the whole project. To build the project, type the following command in terminal:

```bash
tools/sbt assembly
```


## How to run PACC

- You may type "make demo" if you want to just try PACC.
- Hadoop and/or Spark should be installed in your system in advance.

To run PACC, you need to do the followings:
- prepare an edge list file of a graph in a HDFS directory.
  The edge list file is a plain text file where each line is in "NODE1 DELIMITER NODE2" format.
  The delimiter can be a whitespace like a tab or a space.

  For example, this is a sample edge file 'simple.edge'
```
0   2
0   3
1   2
1   3
1   5
2   3
2   4
2   6
4   6
7   8
8   9
7   10
```

- execute PACC on Hadoop

```
Usage: hadoop jar bin/pacc-0.1.jar cc.hadoop.PACC [OPTIONS] [INPUT (EDGE LIST FILE)] [OUTPUT]

Options:
    -DnumPartitions          the number of partitions
    -DlocalThreshold         the threshold for LocalCC optimization (default: 1000000)

Example:
hadoop jar pacc-0.1.jar cc.hadoop.PACC -Dmapred.reduce.tasks=120 -DnumPartitions=120 -DlocalThreshold=20000000 path/to/input/file path/to/output/file
```

- execute PACC on Spark

```
spark-submit --class cc.spark.PACC pacc-0.1.jar [OPTIONS] [INPUT (EDGE LIST FILE)] [OUTPUT]

Options:
    -p                      the number of partitions
    -t                      the threshold for LocalCC optimization (default: 1000000)

Example:
spark-submit --master yarn \
             --deploy-mode client \
             --num-executors 80 \
             --driver-memory 10G \
             --executor-cores 1 --executor-memory 6g \
             --class cc.spark.PACC
             pacc-ext-0.1.jar -p 80 -t 20000000 path/to/input/file path/to/output/file
```


You can test PACC with `bash do_pacc_hadoop.sh` and `bash do_pacc_spark.sh`.

The script executes PACC with a simple graph (simple.edge).
