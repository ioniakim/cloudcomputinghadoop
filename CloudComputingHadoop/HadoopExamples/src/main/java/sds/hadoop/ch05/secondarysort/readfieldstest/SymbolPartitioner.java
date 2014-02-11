package sds.hadoop.ch05.secondarysort.readfieldstest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SymbolPartitioner extends Partitioner<SymbolDateKey, IntWritable> {

	@Override
	public int getPartition(SymbolDateKey key, IntWritable value,
			int numPartitions) {

		int hash = key.getSymbol().hashCode();
 
		return hash % numPartitions;
	}

}
