#!/bin/bash

#export JAVA_TOOL_OPTIONS=-Xmx1g
SCALA_HOME=/opt/scala-2.10.6
SPARK_HOME=/opt/spark-1.6.1-bin-hadoop2.6
export CLASSPATH=.:"$SPARK_HOME/lib/*"

echo --- Deleting
rm SparkWC.jar
rm ece454/SparkWC*.class

echo --- Compiling
$SCALA_HOME/bin/scalac ece454/SparkWC.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
jar -cf SparkWC.jar ece454/SparkWC*.class

echo --- Running
INPUT=sample_input
OUTPUT=output_spark

rm -fr $OUTPUT
$SPARK_HOME/bin/spark-submit --master "local[*]" --class ece454.SparkWC SparkWC.jar $INPUT $OUTPUT

cat $OUTPUT/*
