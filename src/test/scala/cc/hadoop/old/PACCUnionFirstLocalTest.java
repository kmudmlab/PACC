package cc.hadoop.old;

import cc.hadoop.UnionFind;
import cc.hadoop.pacc.old.PACCUnionFirstLocal;
import cc.hadoop.utils.ExternalSorter;
import cc.hadoop.utils.LongPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.StringTokenizer;

import static junit.framework.Assert.assertEquals;

public class PACCUnionFirstLocalTest {

    @Test
    public void testAll() throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Logger.getLogger(ExternalSorter.class).setLevel(Level.ERROR);
        Logger.getLogger(ExternalSorter.LocalPaths.class).setLevel(Level.ERROR);


        URL[] paths = {
                getClass().getResource("/graphs/small/vline"),
                getClass().getResource("/graphs/small/line"),
                getClass().getResource("/graphs/small/facebook_686"),
                getClass().getResource("/graphs/small/w"),
                getClass().getResource("/graphs/facebook"),
                getClass().getResource("/graphs/grqc")
        };

        int[] numPartitionsSet = {/*1, 2, 4, 8,*/ 16};

        int[] localThresholdSet = {0, /*100, 10000*/};

        for(URL path : paths){
            for (int numPartitions : numPartitionsSet) {
                for(int localThreshold : localThresholdSet) {
                    String inputPath = path.getPath();
                    String outputPath = inputPath + ".cc";

                    Configuration conf = new Configuration();

                    conf.setInt("numPartitions", numPartitions);
                    conf.setInt("localThreshold", localThreshold);

                    ToolRunner.run(conf, new PACCUnionFirstLocal(), new String[]{inputPath, outputPath});



                    ArrayList<LongPairWritable> actual = new ArrayList<>();

                    Files.lines(Paths.get(outputPath + "/part-r-00000")).map(line -> {
                        StringTokenizer st = new StringTokenizer(line);
                        long u = Long.parseLong(st.nextToken());
                        long c = Long.parseLong(st.nextToken());
                        return new LongPairWritable(u, c);
                    }).forEach(actual::add);

                    actual.sort(null);



                    ArrayList<LongPairWritable> expected = new ArrayList<>();

                    new UnionFind().run(Files.lines(Paths.get(path.getPath())).map(line -> {
                        StringTokenizer st = new StringTokenizer(line);
                        long u = Long.parseLong(st.nextToken());
                        long v = Long.parseLong(st.nextToken());
                        return new LongPairWritable(u, v);
                    }).iterator()).forEachRemaining(pair -> {
                        expected.add(new LongPairWritable(pair.i, pair.j));
                    });

                    expected.sort(null);


                    assertEquals(expected, actual);


                    FileSystem.get(new Configuration()).delete(new Path(outputPath), true);


                }
            }
        }

    }

}
