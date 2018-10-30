/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * Copyright (c) 2018, Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Seoul National University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * -------------------------------------------------------------------------
 * File: UnionFind.java
 * - A single machine implementation of UnionFind. It finds connected components in a graph.
 */

package cc.hadoop;

import cc.hadoop.utils.LongPairWritable;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Iterator;

public class UnionFind {

    Long2LongOpenHashMap parent;

    public UnionFind(){
        parent = new Long2LongOpenHashMap();
        parent.defaultReturnValue(-1);
    }

    private void union(long a, long b) {
        long r1 = find(a);
        long r2 = find(b);

        if(r1 > r2){
            parent.put(r1, r2);
        }
        else if(r1 < r2){
            parent.put(r2, r1);
        }
    }

    private long find(long x) {

        long p = parent.get(x);
        if(p != -1){
            long new_p = find(p);
            parent.put(x, new_p);
            return new_p;
        }
        else{
            return x;
        }

    }

    public Iterator<LongPairWritable> run(Iterator<LongPairWritable> longPairs){

        while(longPairs.hasNext()){
            LongPairWritable pair = longPairs.next();

            long u = pair.i;
            long v = pair.j;

            if(find(u) != find(v)){
                union(u, v);
            }
        }

        return new Iterator<LongPairWritable>(){

            ObjectIterator<Long2LongMap.Entry> it = parent.long2LongEntrySet().fastIterator();
            LongPairWritable out = new LongPairWritable();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public LongPairWritable next() {
                Long2LongMap.Entry pair = it.next();
                out.i = pair.getLongKey();
                out.j = find(pair.getLongValue());
                return out;
            }
        };

    }


}
