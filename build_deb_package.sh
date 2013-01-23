#!/bin/bash
origdir=$(pwd)
name=kafka-hadoop-consumer
version=$1
: ${version:="0.1.0"}
url=https://github.com/wmf-analytics/kafka-hadoop-consumer
buildroot=build
prefix="/usr"
description="A Kafka Hadoop Consumer that uses ZooKeeper to keep track of Kafka brokers and consumption offset."

#_ MAIN _#
rm -rf ./target
rm -rf ${name}*.deb

#_ MAKE DIRECTORIES _#
rm -rf ${buildroot}
mkdir -p ${buildroot}/${prefix}/{share/java,bin}

# compile
mvn package || exit 1

#_ COPY FILES _#
cp -v target/hadoop_consumer-${version}-SNAPSHOT.jar ${buildroot}/${prefix}/share/java/${name}-${version}.jar
cp -v ./kafka-hadoop-consumer ${buildroot}/${prefix}/bin/kafka-hadoop-consumer

#_ MAKE DEBIAN _#
cd ${buildroot}
fpm -t deb -n $name -v $version --description "$description" --after-install ../after_install.sh --after-remove ../after_remove.sh --url="$url" -a all --prefix=/ -s dir -- .
mv -v ${origdir}/${buildroot}/*.deb ${origdir}
cd ${origdir}

