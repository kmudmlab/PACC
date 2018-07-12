package cc.hadoop.altopt;

public class CopyUtil {

    static long COPY_MASK = 0x4000000000000000L;
    static long HIGH_MASK = 0x8000000000000000L;
    static long LOW_MASK = ~HIGH_MASK;
    static long COPYID_MASK = 0x3FF0000000000000L;
    static long NODEID_MASK = 0xFFFFFFFFFFFFFL;

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
        return n & NODEID_MASK;
    }

    public static long copy(long n, long v, long p){
        return COPY_MASK | ((nodeId(v) % p) << 52) | nodeId(n);
    }

    public static long high(long n){
        return n | HIGH_MASK;
    }

    public static long low(long n){
        return n | LOW_MASK;
    }

}
