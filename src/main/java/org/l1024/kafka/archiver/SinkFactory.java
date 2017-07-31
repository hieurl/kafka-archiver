package org.l1024.kafka.archiver;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import org.l1024.kafka.archiver.s3.S3Sink;

import java.io.IOException;

public class SinkFactory {

    private String s3AccessKey;
    private String s3SecretKey;
    private String s3Bucket;
    private String s3Prefix;

    public SinkFactory(String s3AccessKey, String s3SecretKey, String s3Bucket, String s3Prefix) {
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3Bucket = s3Bucket;
        this.s3Prefix = s3Prefix;
    }

    public S3Sink createSink(String topic, Integer maxMessageCountPerChunk, Integer maxChunkSize, Integer maxCommitInterval) throws IOException {

        AmazonS3Client s3Client;
        if (s3AccessKey != null && !s3AccessKey.isEmpty() && s3SecretKey != null && !s3SecretKey.isEmpty()) {
            s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKey, s3SecretKey));
        } else {
            s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
        }

        return new S3Sink(
                s3Client,
                s3Bucket,
                s3Prefix+"/"+topic,
                topic,
                maxMessageCountPerChunk,
                maxChunkSize,
                maxCommitInterval
        );
    }

}
