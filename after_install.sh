#/bin/sh

# symlink versioned jar to non versioned name.
ln -s $(ls -r /usr/share/java/kafka-hadoop-consumer*.jar | head -n 1) /usr/share/java/kafka-hadoop-consumer.jar