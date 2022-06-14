import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class StripePair implements Writable{

	private String key;
	private int value;
	
	StripePair()
	{
		
	}
	
	StripePair(String s,int n)
	{
		this.key=s;
		this.value=n;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.key=arg0.readUTF();
		this.value=arg0.readInt();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	
		arg0.writeUTF(this.key);
		arg0.writeInt(this.value);
		
	}
	
	public String getKey()
	{
		return this.key;
	}
	
	public void setKey(String s)
	{
		this.key=s;
	}
	
	public int getValue()
	{
		return this.value;
	}
	
	public void setValue(int n)
	{
		this.value=n;
	}
	
	@Override
	public String toString()
	{
		return "{" + this.key+":"+this.value +  "}";
	}
	

}
