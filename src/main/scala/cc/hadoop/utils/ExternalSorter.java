/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: ExternalSorter.java
 * - An external sorter for positive long values.
 */

package cc.hadoop.utils;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import cc.io.DirectWriter;
import cc.io.DirectReader;

/**
 * Positive Long Sorter.
 */
public class ExternalSorter {

    private final static Logger LOGGER = Logger.getLogger(ExternalSorter.class.getName());

    private int bufferSize = 8388608; //64MB
    private long[] values;
    private int array_size;

    public ExternalSorter(String[] basePaths){

        values = new long[bufferSize];

        LOGGER.info(String.format("buffer size    : %d", bufferSize));
        LOGGER.info(String.format("Memory status  : max(%.2fMB) free(%.2fMB)",
                Runtime.getRuntime().maxMemory()/1000000.0,
                availableMemoryBytes(false)/1000000.0));

        LocalPaths.setBasePaths(basePaths);
    }

    public Iterator<Long> sort(Iterator<Long> it) throws IOException {

        String[] inPaths = localSort(it);

        Iterator<Long> lastIterator = new Iterator<Long>() {
            int idx = 0;
            @Override
            public boolean hasNext() {

                if(idx == 0 && idx < array_size) return true;

                while(idx < array_size && values[idx] == values[idx-1]){
                    idx++;
                }

                return idx < array_size;
            }
            @Override
            public Long next() {
                if(hasNext()){
                    return values[idx++];
                }
                else{
                    throw new NoSuchElementException();
                }

            }
        };

        if(inPaths.length == 0) {
            return lastIterator;
        }
        else{
            String mergefirst = LocalPaths.getTempPath("mergefirst");
            mergeTwo(lastIterator,
                    new LongIteratorFromDirectInput(inPaths[inPaths.length-1]),
                    mergefirst);
            inPaths[inPaths.length-1] = mergefirst;

            LOGGER.info(String.format("Merging %d files", inPaths.length));

            return new LongIteratorFromDirectInput(mergeAll(inPaths));

        }





    }

    private String[] localSort(Iterator<Long> it) throws IOException {

        ArrayList<String> inPaths = new ArrayList<>();

        boolean remain = true;

        while (remain) {

            int i = 0;
            try {
                for (; i < bufferSize; i++) {
                    values[i] = it.next();
                }
            } catch (NoSuchElementException e) {
                remain = false;
            }

            array_size = i;

            Arrays.sort(values, 0, i);

            if(remain){
                String tPath = LocalPaths.getTempPath("localsort");

                DirectWriter out = new DirectWriter(tPath);

                long prev = -1;
                for (int j = 0; j < i; j++) {
                    long now = values[j];
                    if(prev != now) out.write(now);
                    prev = now;
                }

                out.close();

                inPaths.add(tPath);
            }
        }

        return inPaths.toArray(new String[0]);
    }

    private String mergeAll(String... inPaths) throws IOException {

        Queue<String> queue = new LinkedList<>();

        Collections.addAll(queue, inPaths);

        while (queue.size() > 1) {

            String in1Path = queue.poll();
            String in2Path = queue.poll();
            String outPath = LocalPaths.getTempPath("mergeall");

            mergeTwo(in1Path, in2Path, outPath);

            Files.delete(Paths.get(in1Path));
            Files.delete(Paths.get(in2Path));


            queue.add(outPath);

        }

        return queue.poll();

    }

    public void print(String in2Path, String outPath) throws IOException {

        BufferedWriter br = new BufferedWriter(new FileWriter(outPath));

        DirectReader in2 = new DirectReader(in2Path);

        while (in2.hasNext()) {
            br.write(in2.readInt() + "\t" + in2.readInt() + "\n");
        }

        in2.close();
        br.close();
    }

    private void mergeTwo(Iterator<Long> in1, Iterator<Long> in2, String outPath) throws IOException {

        DirectWriter out = new DirectWriter(outPath);

        long prev = -1;

        long u, v;

        u = in1.next();
        v = in2.next();

        try {

            while (true) {
                if (u < v) {
                    if (prev != u) {
                        out.write(u);
                        prev = u;
                    }
                    u = in1.next();
                } else if (u > v) {
                    if (prev != v) {
                        out.write(v);
                        prev = v;
                    }
                    v = in2.next();
                } else {
                    u = in1.next();
                }
            }
        } catch (NoSuchElementException e) {
            if (prev != u) {
                out.write(u);
                prev = u;
            } else if (prev != v) {
                out.write(v);
                prev = v;
            }
        }

        try{
            while(true){
                u = in1.next();
                if (prev != u) {
                    out.write(u);
                    prev = u;
                }
            }
        } catch (NoSuchElementException e){/*do nothing*/}

        try{
            while(true){
                u = in2.next();
                if (prev != u) {
                    out.write(u);
                    prev = u;
                }
            }
        } catch (NoSuchElementException e){/*do nothing*/}

        out.close();

    }

    private void mergeTwo(String in1Path, String in2Path, String outPath) throws IOException {

        DirectReader in1 = new DirectReader(in1Path);
        DirectReader in2 = new DirectReader(in2Path);
        DirectWriter out = new DirectWriter(outPath);

        long prev = -1;

        long u, v;

        u = in1.readLong();
        v = in2.readLong();

        try {

            while (true) {
                if (u < v) {
                    if (prev != u) {
                        out.write(u);
                        prev = u;
                    }
                    u = in1.readLong();
                } else if (u > v) {
                    if (prev != v) {
                        out.write(v);
                        prev = v;
                    }
                    v = in2.readLong();
                } else {
                    u = in1.readLong();
                }
            }
        } catch (EOFException e) {
            if (prev != u) {
                out.write(u);
                prev = u;
            } else if (prev != v) {
                out.write(v);
                prev = v;
            }
        }

        while (in1.hasNext()) {
            u = in1.readLong();
            if (prev != u) {
                out.write(u);
                prev = u;
            }
        }

        while (in2.hasNext()) {
            v = in2.readLong();
            if (prev != v) {
                out.write(v);
                prev = v;
            }
        }


        out.close();
        in2.close();
        in1.close();

    }

    static public class LocalPaths {

        private final static Logger LOGGER = Logger.getLogger(LocalPaths.class.getName());

        private static final Random rand = new Random();

        private static String[] basePaths;

        static void setBasePaths(String[] basePaths) {

            LOGGER.info("Basepaths: " + Arrays.toString(basePaths));

            for (String tmpDir : basePaths) {
                new File(tmpDir).mkdirs();
            }

            LocalPaths.basePaths = basePaths;
        }

        private static String getNextBasePath() {
            return basePaths[rand.nextInt(basePaths.length)];
        }

        static String getTempPath(String prefix) {

            String res = null;
            do {
                int t = rand.nextInt(Integer.MAX_VALUE);
                res = getNextBasePath() + "/" + prefix + "-" + t;
            } while (Files.exists(Paths.get(res)));

            return res;

        }

    }

    private long availableMemoryBytes(boolean gc){

        if(gc){
            System.gc(); System.gc();
        }

        return Runtime.getRuntime().maxMemory()
                - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

}
