package org.l1024.kafka.archiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

public interface Sink {

    public boolean append(ConsumerRecord consumerRecord, boolean hasPostCommitAction)
            throws IOException;

    public Long getCommittedOffset();

    public void postCommitFinished();

    public boolean maybeCommitChunk(boolean hasPostCommitAction) throws IOException;

    public long getUncommittedMessageSize();

    public long getLastCommitTimestamp();

    public TopicPartition getPartition();
}