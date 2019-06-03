#!/bin/bash

mkdir _build
tar xvf hadoop-3.1.2-src.tar.gz -C _build
cd _build/hadoop-3.1.2-src/hadoop-hdfs-project/
mvn clean compile package -Pdist,native -DskipTests -Dmaven.javadoc.skip=true

cd ../../..
mkdir -p _install/include/hdfs _install/lib/

cp _build/hadoop-3.1.2-src/hadoop-hdfs-project/hadoop-hdfs-native-client/target/hadoop-hdfs-native-client-3.1.2/include/hdfs.h _install/include/hdfs
cp _build/hadoop-3.1.2-src/hadoop-hdfs-project/hadoop-hdfs-native-client/target/hadoop-hdfs-native-client-3.1.2/lib/native/* _install/lib/
