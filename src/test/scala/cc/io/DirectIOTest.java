package cc.io;

import cc.hadoop.utils.LongIteratorFromDirectInput;
import org.junit.Test;

import java.util.NoSuchElementException;

public class DirectIOTest {

    @Test
    public void testAll() throws Exception {

        String tmpPath = "tttttttt/aaa";

        DirectWriter dw = new DirectWriter(tmpPath, 16);

        for (long i = 0; i < 1000; i++) {
            dw.write(i);
        }

        dw.close();

//        DirectReader dr = new DirectReader(tmpPath, 8);
//
//        while(dr.hasNext()){
//            System.out.println(dr.readLong());
//        }
//
//        for(int i=0; i<10; i++){
//            System.out.println(dr.readLong());
//        }


        LongIteratorFromDirectInput it = new LongIteratorFromDirectInput(tmpPath);

        try {
            for (int i = 0; i < 1004; i++) {
                System.out.println(it.next());
            }
        } catch (NoSuchElementException e){
            System.out.println("??");
//            e.printStackTrace();
        }

        System.out.println(it.hasNext());
        System.out.println(it.hasNext());
        System.out.println(it.hasNext());
        System.out.println(it.hasNext());
        System.out.println(it.hasNext());

    }
}