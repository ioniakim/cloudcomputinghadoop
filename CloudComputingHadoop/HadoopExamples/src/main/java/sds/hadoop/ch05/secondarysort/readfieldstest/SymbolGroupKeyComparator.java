package sds.hadoop.ch05.secondarysort.readfieldstest;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SymbolGroupKeyComparator extends WritableComparator {

	protected SymbolGroupKeyComparator() {
		super(SymbolDateKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SymbolDateKey key1 = (SymbolDateKey) a;
		SymbolDateKey key2 = (SymbolDateKey) b;
		
		
		return key1.getSymbol().compareTo(key2.getSymbol());
	}
}
