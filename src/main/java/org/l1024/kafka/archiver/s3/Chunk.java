package org.l1024.kafka.archiver.s3;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;

public abstract class Chunk {

    protected String topic;

    protected long endOffset;

    protected long totalMessageCount = 0;
    protected long totalMessageSize = 0;

    protected Chunk(String topic) throws IOException {
        this.topic = topic;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getTotalMessageCount() {
        return totalMessageCount;
    }

    public long getTotalMessageSize() {
        return totalMessageSize;
    }

    public abstract void appendMessageToChunk(ConsumerRecord messageAndOffset) throws IOException;

    public abstract File finalizeChunk() throws IOException;

    public abstract void cleanUp();

    public boolean isEmpty() {
        return totalMessageCount == 0;
    }
}
