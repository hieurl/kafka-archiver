# kafka-archiver

## Description

Archive Kafka messages into sequence files in S3.
All messages of a topicPartition will be stored in sequence files under one directory. Each sequence file will contain a chunk of messages.

### Configurable options
- topics which should be archived
- total message size per chunk
- s3 bucket/prefix
- maximum interval between chunk uploads
- ignore gaps setting per topicPartition (by default the archiver fails, if there is a gap in the message stream)

With 'mybucket' and 'myarchive' configured as s3 bucket/prefix you will end up with the following files in s3:
<tobe update>

### Build
```
gradle jar
```

creates build/libs/kafka-archiver-0.1.jar

### Start
```
java -jar kafka-backup-0.1.jar kafkaconfig.properties serverconfig.properties 
```

### Stop/Restart

It is safe to just kill and restart the daemon. It will resume its operation where it left off.
