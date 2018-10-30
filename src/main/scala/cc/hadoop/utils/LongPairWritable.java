/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: LongPairWritable.java
 * - Writable for a long pair.
 */

package cc.hadoop.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class LongPairWritable implements WritableComparable<LongPairWritable>{

	public long i, j;

	public LongPairWritable(){}
	
	public LongPairWritable(long i, long j){ set(i, j);}

	public void set(long i, long j) {
		this.i = i;
		this.j = j;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readLong();
		j = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(i);
		out.writeLong(j);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (i ^ (i >>> 32));
		result = prime * result + (int) (j ^ (j >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof LongPairWritable)) return false;
		LongPairWritable other = (LongPairWritable) obj;
		if(i != other.i) return false;
		if(j != other.j) return false;
		return true;
	}

	@Override
	public int compareTo(LongPairWritable o) {
		
		long thisI = this.i;
		long thisJ = this.j;
		long thatI = o.i;
		long thatJ = o.j;

		int compI = Long.compare(thisI, thatI);

		if (compI != 0) return compI;
		else return Long.compare(thisJ, thatJ);
	}

	@Override
	public String toString() {
		return i + "\t" + j;
	}
	
	/** A Comparator optimized for LongWritable. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(LongPairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			long thisI = readLong(b1, s1);
			long thatI = readLong(b2, s2);

			if(thisI < thatI) return -1;
			else if(thisI > thatI) return 1;
			else{

				long thisJ = readLong(b1, s1 + 8);
				long thatJ = readLong(b2, s2 + 8);

				return Long.compare(thisJ, thatJ);

			}

		}
	}
	
	@SuppressWarnings("rawtypes")
	/** A decreasing Comparator optimized for LongWritable. */
	public static class DecreasingComparator extends Comparator {

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return super.compare(b, a);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return super.compare(b2, s2, l2, b1, s1, l1);
		}
	}
	
	static{ // register default comparator
		WritableComparator.define(LongPairWritable.class, new Comparator());
	}

}
