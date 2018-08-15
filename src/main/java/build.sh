#!/bin/sh
ITERATION=1
BETA=0.2
#compile
hadoop com.sun.tools.javac.Main *.java

# packing
jar cf pagerank.jar *.class

# create folder
hdfs dfs -rm -r input/
hdfs dfs -mkdir -p input/transition
hdfs dfs -mkdir -p input/pagerank0

# copy to input
hdfs dfs -put pr.txt input/pagerank0/
hdfs dfs -put transition.txt input/transition/

# run
hadoop jar pagerank.jar Driver input/transition input/pagerank input/unitState $ITERATION $BETA
