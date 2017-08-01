package org.l1024.kafka.archiver.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.l1024.kafka.archiver.Sink;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class S3Sink implements Sink {

    static final Logger logger = Logger.getLogger(S3Sink.class);

    private AmazonS3Client s3Client;

    private final String bucket;
    private final String keyPrefix;

    protected String topic;
    private long maxCommittedOffset;

    private long committedMessageCount = 0;
    private long committedMessageSize = 0;

    private long lastCommitTimestamp;
    private int maxNumberRecordPerChunk;
    private int maxCommitChunkSize;
    private int maxCommitInterval;

    private Map<Integer, Long> committedOffsets; // partition

    private Map<Integer, Long> currentOffset;

    private Chunk chunk;

    private Semaphore appendCommitSemaphore;
    private Semaphore postCommitSemaphore;

    public S3Sink(
            AmazonS3Client s3Client,
            String bucket,
            String keyPrefix,
            String topic,
            Integer maxMessageCountPerChunk,
            Integer maxChunkSize,
            Integer maxCommitInterval
    ) throws IOException {

        this.s3Client = s3Client;

        this.bucket = bucket;
        this.topic = topic;
        this.keyPrefix = keyPrefix + "/" + this.topic + "_";

        lastCommitTimestamp = System.currentTimeMillis();

        chunk = TextFileChunk.createChunk(topic);

        this.maxNumberRecordPerChunk = maxMessageCountPerChunk;
        this.maxCommitInterval = maxCommitInterval;
        this.maxCommitChunkSize = maxChunkSize;

        this.committedOffsets = new HashMap<>();
        this.currentOffset = new HashMap<>();

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
            if (getUncommittedMessageCount() > maxNumberRecordPerChunk) {
                logger.info(String.format("Committing chunk for %s. (count)", this.topic));
                commitChunk(hasPostCommitAction);
            } else if (getUncommittedMessageSize() > maxCommitChunkSize) {
                logger.info(String.format("Committing chunk for %s. (size)", this.topic));
                commitChunk(hasPostCommitAction);
            } else if (System.currentTimeMillis() - getLastCommitTimestamp() > maxCommitInterval) {
                logger.info(String.format("Committing chunk for %s. (interval)", topic));
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
        currentOffset.put(consumerRecord.partition(), consumerRecord.offset()+1);
    }

    @Override
    public Map<Integer, Long> getCommittedOffsets() {
        return committedOffsets;
    }

    public void commitChunk(boolean hasPostCommitAction) throws IOException {
        try {
            appendCommitSemaphore.take();
            if (hasPostCommitAction) postCommitSemaphore.take();

            long endOffset = chunk.getEndOffset();

            lastCommitTimestamp = System.currentTimeMillis();

            if (chunk.isEmpty()) {
                logger.debug("Empty chunk. Nothing to upload.");
                return;
            }

            File tmpChunkFile = chunk.finalizeChunk();

            String key = keyPrefix + String.format("%019d",endOffset);
            logger.info(String.format("Uploading chunk to S3 (%s).", key));
            s3Client.putObject(bucket, key, tmpChunkFile);

            maxCommittedOffset = endOffset;
            committedMessageCount += chunk.getTotalMessageCount();
            committedMessageSize += chunk.getTotalMessageSize();

            chunk.cleanUp();

            chunk = TextFileChunk.createChunk(topic);

            this.committedOffsets = new HashMap<>(currentOffset);
            logger.info(this.committedOffsets.toString());
        } catch (InterruptedException e) {
            logger.error(e);
            return;
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
            logger.info(String.format("Committing chunk for %s. (interval)", topic));
            commitChunk(hasPostCommitAction);
            return true;
        }
        return false;
    }

    @Override
    public long getMaxCommittedOffset() {
        return maxCommittedOffset;
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
