FROM probablyfine/flume

COPY --from=bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 /opt/hadoop-3.2.1 /opt/hadoop-3.2.1

RUN rm -f /opt/flume/lib/guava-11.0.2.jar

ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_IN_PATH=/opt/hadoop-3.2.1/bin/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop-3.2.1/etc/hadoop
