package cc.hadoop.altopt;

public class CopyUtil {

    static long COPY_MASK = 0x1L;
    static long HIGH_MASK = 0x8000000000000000L;
    static long LOW_MASK = ~HIGH_MASK;
    static long COPYID_MASK = 0x7FE0000000000000L;
    static long NODEID_MASK = 0x1FFFFFFFFFFFFEL;
    static long COMP_MASK = 0x1FFFFFFFFFFFFFL;

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
        return (n & NODEID_MASK) >> 1;
    }

    public static long copy(long n, long v, long p){
        return COPY_MASK | ((nodeId(v) % p) << 53) | (nodeId(n) << 1);
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
        return (n << 1) >>> 53;
    }

    public static String tuple(long n){
        return nodeId(n) + (isHigh(n) ? "h" : "") + (isCopy(n) ? "c" + copyId(n) : "" );
    }

}
