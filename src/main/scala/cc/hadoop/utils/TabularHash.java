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
 * File: TabularHash.java
 * - A 4-wise independent family of functions implemented by tabular hashing.
 */

package cc.hadoop.utils;


import java.util.Random;

public class TabularHash {

    private int[] t0, t1, t2, t3;

    private static TabularHash instance;
    private TabularHash () {

        Random rand = new Random(0);

        int table_size = 65536;

        t0 = new int[table_size];
        t1 = new int[table_size];
        t2 = new int[table_size];
        t3 = new int[table_size];

        for(int i = 0; i < table_size; i++){
            t0[i] = rand.nextInt(Integer.MAX_VALUE);
            t1[i] = rand.nextInt(Integer.MAX_VALUE);
            t2[i] = rand.nextInt(Integer.MAX_VALUE);
            t3[i] = rand.nextInt(Integer.MAX_VALUE);
        }

    }

    public static TabularHash getInstance(){
        if(instance == null){
            instance = new TabularHash();
        }
        return instance;
    }

    public int hash(long x){

        return t0[(int) (0xFFFF & x)] ^ t1[(int) (0xFFFF & (x >>> 16))] ^
                t2[(int) (0xFFFF & (x >>> 32))] ^ t3[(int) (0xFFFF & (x >>> 48))];

    }

}
