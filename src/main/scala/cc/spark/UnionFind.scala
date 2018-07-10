package cc.spark

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object UnionFind{

  /**
    * Find connected components.
    * @param edges the edges of an input graph
    * @return iterator of connected components
    */
  def run(edges: Iterator[(Long, Long)]): Iterator[(Long, Long)] ={

    val parent = new mutable.LongMap[Long]().withDefaultValue(-1)

    // find the minimum node rechable from node x.
    def find(x: Long): Long = {
      val res = parent.get(x) match {
        case None => x
        case Some(p) =>
          val new_p = find(p)
          parent(x) = new_p
          new_p
      }

      res
    }

    // union two connected components concerning the nodes in edge (x, y)
    def union(x: Long, y: Long): Unit ={
      val r1 = find(x)
      val r2 = find(y)

      if(r1 > r2)
        parent(r1) = r2
      else if(r1 < r2)
        parent(r2) = r1
    }

    for( (u, v) <- edges){
      if(find(u) != find(v)){
        union(u, v)
      }
    }

    parent.map{case (x,y) => (x, find(x))}.iterator

  }

  // interface for RDD
  def run(out: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    val arr = out.collect().iterator
    out.sparkContext.parallelize[(Long, Long)](run(arr).toStream)
  }


}
