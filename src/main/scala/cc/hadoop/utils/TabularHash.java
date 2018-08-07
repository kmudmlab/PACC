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
