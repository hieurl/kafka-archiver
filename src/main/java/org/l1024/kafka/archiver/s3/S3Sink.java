package org.l1024.kafka.archiver.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.l1024.kafka.archiver.Sink;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class S3Sink implements Sink {

    static final Logger logger = Logger.getLogger(S3Sink.class);

    private AmazonS3Client s3Client;

    private final String bucket;
    private final String keyPrefix;

    protected TopicPartition partition;

    private long committedMessageCount = 0;
    private long committedMessageSize = 0;

    private long lastCommitTimestamp;
    private int maxNumberRecordPerChunk;
    private int maxCommitChunkSize;
    private int maxCommitInterval;

    private long committedOffset; // partition

    private long currentOffset;

    private Chunk chunk;
    private long chunkInitTimestamp;

    private Semaphore appendCommitSemaphore;
    private Semaphore postCommitSemaphore;

    public S3Sink(
            AmazonS3Client s3Client,
            String bucket,
            String keyPrefix,
            TopicPartition partition,
            Integer maxMessageCountPerChunk,
            Integer maxChunkSize,
            Integer maxCommitInterval
    ) throws IOException {

        this.s3Client = s3Client;

        this.bucket = bucket;
        this.partition = partition;
        this.keyPrefix = keyPrefix;

        lastCommitTimestamp = System.currentTimeMillis();

        chunk = TextFileChunk.createChunk(this.partition.toString());
        chunkInitTimestamp = -1;

        this.maxNumberRecordPerChunk = maxMessageCountPerChunk;
        this.maxCommitInterval = maxCommitInterval;
        this.maxCommitChunkSize = maxChunkSize;

        this.committedOffset = -1;
        this.currentOffset = -1;

        this.appendCommitSemaphore = new Semaphore();
        this.postCommitSemaphore = new Semaphore();
    }

    @Override
    public boolean append(ConsumerRecord consumerRecord, boolean hasPostCommitAction) throws IOException {
        try {
            appendCommitSemaphore.take();
            chunk.appendMessageToChunk(consumerRecord);
            updateOffset(consumerRecord);
            appendCommitSemaphore.release();
            if (getUncommittedMessageCount() >= maxNumberRecordPerChunk) {
                logger.info(String.format("Committing chunk for %s. (count)", this.partition.toString()));
                commitChunk(hasPostCommitAction);
            } else if (getUncommittedMessageSize() > maxCommitChunkSize) {
                logger.info(String.format("Committing chunk for %s. (size)", this.partition.toString()));
                commitChunk(hasPostCommitAction);
            } else if (System.currentTimeMillis() - getLastCommitTimestamp() > maxCommitInterval) {
                logger.info(String.format("Committing chunk for %s. (interval)", partition.toString()));
                commitChunk(hasPostCommitAction);
            } else {
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            logger.error(e);
            return false;
        }
    }

    protected void updateOffset(ConsumerRecord consumerRecord) {
        if (chunkInitTimestamp == -1) chunkInitTimestamp = consumerRecord.timestamp();
        currentOffset=consumerRecord.offset();
    }

    @Override
    public Long getCommittedOffset() {
        return committedOffset;
    }

    public void commitChunk(boolean hasPostCommitAction) throws IOException {
        try {
            appendCommitSemaphore.take();
            if (hasPostCommitAction) postCommitSemaphore.take();

            long endOffset = chunk.getEndOffset();

            lastCommitTimestamp = System.currentTimeMillis();

            if (chunk.isEmpty()) {
                logger.info("Empty chunk. Nothing to upload.");
                return;
            }

            File tmpChunkFile = chunk.finalizeChunk();

            String key = keyPrefix + "/" +
                    new SimpleDateFormat("yyyy.MM.dd").format(new Date(chunkInitTimestamp)) + "/" +
                    chunk.topicPartition + "_" + String.format("%019d",endOffset);
            logger.info(String.format("Uploading chunk to S3 (%s).", key));
            s3Client.putObject(bucket, key, tmpChunkFile);

            committedMessageCount += chunk.getTotalMessageCount();
            committedMessageSize += chunk.getTotalMessageSize();

            chunk.cleanUp();

            chunk = TextFileChunk.createChunk(partition.toString());
            chunkInitTimestamp = -1;

            this.committedOffset = currentOffset;
            logger.info(String.format("Uploaded backup for %s-%d at offset %d", this.partition.topic(), this.partition.partition(), committedOffset));
        } catch (InterruptedException e) {
            logger.error(e);
        } finally {
            appendCommitSemaphore.release();
        }
    }

    @Override
    public void postCommitFinished() {
        postCommitSemaphore.release();
    }

    @Override
    public boolean maybeCommitChunk(boolean hasPostCommitAction) throws IOException {
        if (System.currentTimeMillis() - getLastCommitTimestamp() > maxCommitInterval) {
            logger.info(String.format("Committing chunk for %s. (interval)", partition.toString()));
            commitChunk(hasPostCommitAction);
            return true;
        }
        return false;
    }

    @Override
    public long getUncommittedMessageSize() {
        return chunk.getTotalMessageSize();
    }

    public long getUncommittedMessageCount() {
        return chunk.getTotalMessageCount();
    }

    @Override
    public long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    @Override
    public TopicPartition getPartition() {
        return this.partition;
    }

    @Override
    public String toString() {
        return String.format("S3Sink(bucket=%s,prefix=%s,committedMessageCount=%d,committedMessageSize=%d,uncommittedMessageCount=%d,uncommittedMessageSize=%d,endOffset=%d)",bucket, keyPrefix, committedMessageCount, committedMessageSize, chunk.getTotalMessageCount(), chunk.getTotalMessageSize(), chunk.getEndOffset());
    }

    public class Semaphore {
        private boolean taken = false;

        public synchronized void take() throws InterruptedException{
            while(this.taken) wait();
            this.taken = true;
        }

        public synchronized void release() {
            this.taken = false;
            this.notify();
        }

    }
}
