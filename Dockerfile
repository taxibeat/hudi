FROM openjdk:8u212-jdk as builder
RUN apt-get update && apt-get install maven -y
COPY . .
RUN mvn clean package -DskipTests -Dscala-2.12 -Dspark3

FROM harbor.mgmt.bigdata.thebeat.co/beat-bigdata/spark:v3.1.1-hadoop3.2

USER root

COPY --from=builder hudi-common/target/classes/org/apache/hudi/common/model/DebeziumAvroPayload.class /opt/spark/jars/
COPY --from=builder packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.12-0.7.0.jar /opt/spark/jars/
COPY --from=builder packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.7.0.jar /opt/spark/jars/
COPY --from=builder packaging/hudi-hadoop-mr-bundle/target/hudi-hadoop-mr-bundle-0.7.0.jar /opt/spark/jars/

RUN apt-get update && apt-get install wget -y

## Download AWS jars
RUN wget -O aws-java-sdk-bundle-1.11.375.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar && mv aws-java-sdk-bundle-1.11.375.jar /opt/spark/jars/
RUN wget -O hadoop-aws-3.2.0.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && mv hadoop-aws-3.2.0.jar /opt/spark/jars/

## Download Spark Avro jars
RUN wget -O spark-avro_2.12-3.1.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.1/spark-avro_2.12-3.1.1.jar && mv spark-avro_2.12-3.1.1.jar /opt/spark/jars/

## Download extra misc deps
RUN wget -O jets3t-0.9.4.jar https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar && mv jets3t-0.9.4.jar /opt/spark/jars/

# User 185 is defined in the base image by Spark.
USER 185