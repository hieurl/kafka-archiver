package org.l1024.kafka.archiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;

public interface Sink {

    public boolean append(ConsumerRecord consumerRecord, boolean hasPostCommitAction)
            throws IOException;

    public Map<Integer, Long> getCommittedOffsets();

    public void postCommitFinished();

    public boolean maybeCommitChunk(boolean hasPostCommitAction) throws IOException;

    public long getMaxCommittedOffset();

    public long getUncommittedMessageSize();

    public long getLastCommitTimestamp();

}