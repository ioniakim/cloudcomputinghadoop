#!/usr/bin/env sh

# hadoop-streaming.jar for streaming
# -input: input data
# -output: result directory
# -mapper: mapper script
# -reducer: reducer script
# -combiner: combiner script
# -file: script to ship on the hadoop cluster.

hadoop jar $HADOOP_INSTALL/contrib/streaming/hadoop-streaming-1.2.1.jar -input stock/data -output stock/streaming_combiner_output -mapper ./stockrisemap.py -reducer ./stockrisereduce.py -combiner ./stockrisereduce.py -file ./stockrisemap.py -file ./stockrisereduce.py

