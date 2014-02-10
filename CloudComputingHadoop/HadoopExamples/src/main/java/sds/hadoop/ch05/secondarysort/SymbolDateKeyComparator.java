/**
 * 
 */
package sds.hadoop.ch05.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author ionia
 *
 */
public class SymbolDateKeyComparator extends WritableComparator {

	protected SymbolDateKeyComparator() {
		super(SymbolDateKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SymbolDateKey key1 = (SymbolDateKey) a; 
		SymbolDateKey key2 = (SymbolDateKey) b;
		
		return key1.compareTo(key2);
	}
}
