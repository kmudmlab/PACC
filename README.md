# PACC

PACC (Partition Aware Connected Components) is a tool for computing connected components in a large graph.
It runs in parallel, distributed manner on top of Hadoop and Spark, which are widely-used distributed computation system.

## Build

PACC uses [SBT (Simple Build Tool)](http://www.scala-sbt.org/) to manage dependencies and build the whole project. To build the project, type the following command in terminal:

```bash
sbt assembly
```

