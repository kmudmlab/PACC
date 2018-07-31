package cc.hadoop.altopt;

public class CopyUtil {


    /*
     * high bit               node id (13 * 4 bits)      copy bit    copy id (10 bits)
     * ↓                         ↓                            ↓        ↓
     * - ---------------------------------------------------- - ----------
     * 0 0000000000000000000000000000000000000000000000000000 0 0000000000
     */

    private static long HIGH_POSITION = 63;
    private static long NODEID_POSITION = 11;
    private static long COPY_POSITION = 10;
    private static long COPYID_POSITION = 0;

    private static long HIGH_MASK = 1L << HIGH_POSITION;
    private static long COPY_MASK = 1L << COPY_POSITION;
    private static long LOW_MASK = ~HIGH_MASK;
    private static long COPYID_MASK = 0x3FFL << COPYID_POSITION;
    private static long NODEID_MASK = 0xFFFFFFFFFFFFFL << NODEID_POSITION;
    private static long COMP_MASK = NODEID_MASK | COPY_MASK | COPYID_MASK;

    public static long toNode(long n){
        return n << NODEID_POSITION;
    }


    public static boolean isHigh(long n) {
        return (n & HIGH_MASK) != 0;
    }

    public static boolean isCopy(long n){
        return (n & COPY_MASK) != 0;
    }

    public static boolean nonCopy(long n) {
        return (n & COPY_MASK) == 0;
    }

    public static long nodeId(long n){
        return (n & NODEID_MASK) >>> NODEID_POSITION;
    }

    public static long copy(long n, long v, long p){
        return COPY_MASK | ((nodeId(v) % p) << COPYID_POSITION) | (nodeId(n) << NODEID_POSITION);
    }

    public static long high(long n){
        return n | HIGH_MASK;
    }

    public static long low(long n){
        return n & LOW_MASK;
    }

    public static long comp(long n){
        return n & COMP_MASK;
    }

    public static long copyId(long n){
        return (n & COPYID_MASK) << COPYID_POSITION;
    }

    public static String tuple(long n){
        return nodeId(n) + (isHigh(n) ? "h" : "") + (isCopy(n) ? "c" + copyId(n) : "" );
    }

    public static int hash(long n) {

        return Long.hashCode(nodeId(n) + 41 * copyId(n));

    }


}
