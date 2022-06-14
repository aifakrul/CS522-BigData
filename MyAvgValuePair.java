import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyAvgValuePair implements Writable{

	private long size;
	private long count;

	MyAvgValuePair()
	{
		
	}
	
	MyAvgValuePair(long s, long n) {
		this.size = s;
		this.count = n;
	}

	public long getSize() {
		return this.size;
	}

	public long getCount() {
		return this.count;
	}

	public void setSize(long s)
	{
		this.size = s;
	}
	
	public void setCount(long n)
	{
		this.count = n;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.size = in.readLong();
		this.count = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.size);
		out.writeLong(this.count);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (size ^ (size >>> 32));
		result = prime * result + (int) (count ^ (count >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof MyAvgValuePair)) {
			return false;
		}

		MyAvgValuePair other = (MyAvgValuePair) obj;
		if (this.getSize() != other.getSize()
				|| this.getCount() != other.getCount()) {
			return false;
		}

		return true;
	}

	@Override
	public String toString() {
		return this.size + "," + this.count;
	}
}