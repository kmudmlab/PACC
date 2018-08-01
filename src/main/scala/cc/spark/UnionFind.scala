package cc.spark

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectIterator
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object UnionFind{

  /**
    * Find connected components.
    * @param edges the edges of an input graph
    * @return iterator of connected components
    */
  def run(edges: Iterator[(Long, Long)]): Iterator[(Long, Long)] ={

    val parent = new Long2LongOpenHashMap
    parent.defaultReturnValue(-1)

    // find the minimum node rechable from node x.
    def find_root(x: Long): Long = {
      val p = parent.get(x)
      if(p != -1){
        val new_p = find_root(p)
        parent.put(x, new_p)
        new_p
      }
      else x
    }

    // union two connected components concerning the nodes in edge (x, y)
    def union(x: Long, y: Long): Unit ={
      val r1 = find_root(x)
      val r2 = find_root(y)

      if(r1 > r2)
        parent.put(r1, r2)
      else if(r1 < r2)
        parent.put(r2, r1)
    }

    for( (u, v) <- edges){
      if(find_root(u) != find_root(v)){
        union(u, v)
      }
    }

    new Iterator[(Long, Long)] {

      val it = parent.long2LongEntrySet.fastIterator

      override def hasNext: Boolean = it.hasNext


      override def next(): (Long, Long) = {
        val pair = it.next()
        (pair.getLongKey, find_root(pair.getLongValue))
      }
    }


  }

  // interface for RDD
  def run(out: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    val arr = out.collect().iterator
    out.sparkContext.parallelize[(Long, Long)](run(arr).toStream)
  }

  def spanningForest(edges: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {

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

    edges.filter{case (u, v) =>
        if(find(u) != find(v)){
          union(u, v)
          true
        }
        else false
    }

  }


}
