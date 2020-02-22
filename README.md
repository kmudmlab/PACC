
# PACC

- PACC (Partition Aware Connected Components) is a tool for computing connected components in a large graph.
- Given an undirected simple graph, PACC returns the minimum reachable node id for each node.
- It runs in parallel, distributed manner on top of Hadoop and Spark, which are widely-used distributed computation system.
- Authors:
  - Ha-Myung Park (hmpark@kookmin.ac.kr), Kookmin University
  - Namyong Park (namyongp@cs.cmu.edu), Carnegie Mellon University
  - Sung-Hyon Myaeng (myaeng@kaist.ac.kr), KAIST
  - U Kang (ukang@snu.ac.kr), Seoul National University

- If your work uses or refers to PACC, please cite the paper using the following bibtex entry:
    ```
    @inproceedings{ParkPACC16,
      author = {Ha-Myung Park and
                   Namyong Park and
                   Sung-Hyon Myaeng and
                   U Kang},
      title = {Partition Aware Connected Component Computation in Distributed Systems},
      booktitle = {ICDM},
      year = {2016},
    }
    ```

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

## Real world datasets

| Name        | #Nodes      | #Edges        | Description                                                 | Source                           |
|-------------|-------------|---------------|-------------------------------------------------------------|----------------------------------|
| Skitter     | 1,696,415   | 11,095,298    | Internet topology graph, from traceroutes run daily in 2005 | [SNAP](http://snap.stanford.edu/data/as-skitter.html)                             |
| Patent      | 3,774,768   | 16,518,948    | Citation network among US Patents                           | [SNAP](http://snap.stanford.edu/data/cit-Patents.html)                             |
| LiveJournal | 4,847,571   | 68,993,773    | LiveJournal online social network                           | [SNAP](http://snap.stanford.edu/data/soc-LiveJournal1.html)                             |
| Friendster  | 65,608,366  | 1,806,067,135 | Friendster online social network                            | [SNAP](http://snap.stanford.edu/data/com-Friendster.html)                             |
| Twitter     | 41,652,230  | 1,468,365,182 | Twitter follower-followee network                           | [Advanced Networking Lab at KAIST](http://an.kaist.ac.kr/traces/WWW2010.html) |
| SubDomain   | 89,247,739  | 2,043,203,933 | Domain level hyperlink network on the Web                   | [Yahoo Webscope](http://webscope.sandbox.yahoo.com/)                   |
| YahooWeb    | 720,242,173 | 6,636,600,779 | Page level hyperlink network on the Web                     | [Yahoo Webscope](http://webscope.sandbox.yahoo.com/)                   |

## Synthetic datasets
RMAT graphs are generated using parameters (a, b, c, d) = (0.57, 0.19, 0.19, 0.05).
| Name      | #Nodes      | #Edges        | #Edges/#nodes |
|-----------|-------------|---------------|---------------|
| RMAT-S-21 | 731,258     | 29,519,203    | 40.36         |
| RMAT-S-23 | 2,735,400   | 120,517,935   | 44.05         |
| RMAT-S-25 | 10,204,129  | 488,843,429   | 47.90         |
| RMAT-S-27 | 38,034,673  | 1,974,122,517 | 51.90         |
| RMAT-S-29 | 141,509,689 | 7,947,695,690 | 56.16         |
| RMAT-D-21 | 979,581     | 379,966,649   | 387.88        |
| RMAT-D-23 | 3,377,251   | 455,483,172   | 134.86        |
| RMAT-D-25 | 10,208,216  | 488,846,951   | 47.88         |
| RMAT-D-27 | 26,585,821  | 499,793,060   | 18.79         |
| RMAT-D-29 | 59,716,543  | 502,594,547   | 8.41          |


