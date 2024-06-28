# Huawei Cloud DMS for Kafka Java demo package

Based on Java demo package available at
<https://support.huaweicloud.com/intl/en-us/devg-kafka/how-to-connect-kafka.html>
and code available at
<https://support.huaweicloud.com/intl/en-us/devg-kafka/Kafka-java-demo.html>.

## Instructions

1. Create a DMS for Kafka instance with the following configuration:
   - Version: 3.x;
   - Kafka SALS_SSL: enabled;
   - SALS/PLAIN: disabled;
   - Advanced Settings / Public Access: enable if accessing from outside
     Huawei Cloud, then assign 3 EIPs;
   - Advanced Settings / Automatic Topic Creation: enable;
   - Configure other parameters as required and finish creation;
   - Wait until instance is Running;
2. Clone this repository;
3. On Basic Information page / Connection section, download the SSL Certificate
   file (named `kafka-certs.zip`);
4. Extract `client.jks` file from `kafka-certs.zip` into `src/main/resources`
   folder;
5. In folder `src/main/resources`, make copies of files
   `dms.sdk.consumer.properties.example` and
   `dms.sdk.producer.properties.example`, removing `.example` from the end;
6. On those two files, update the following properties:
   - `bootstrap.servers`: use Instance Addresses displayed in Basic
     Information page / Connection section;
   - `sasl.jaas.config`: change `username` and `password` with values set when
     creating the instance;
   - `ssl.truststore.location`: set the absolute path to the `client.jks` file;
9. Run command `mvn test` or run manually methods `testProducer()`
   and `testConsumer()`