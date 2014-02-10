package sds.hadoop.ch05.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SymbolDateKey implements WritableComparable<SymbolDateKey>{
	
	private String symbol;
	private Integer date;
	


	public Integer getDate() {
		return date;
	}

	public void setDate(Integer date) {
		this.date = date;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = WritableUtils.readString(in);
		date = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, symbol);
		out.writeInt(date);
	}

	@Override
	public int compareTo(SymbolDateKey key) {
		int result = symbol.compareTo(key.getSymbol());
		if(0 == result){
			result = date.compareTo(key.getDate());
		}
		return result;
	}

	@Override
	public String toString() {

		return symbol + ":" + getDate().toString();
	}
}
