package org.l1024.kafka.archiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;

class MessageStream {

    private static final Logger logger = Logger.getLogger(MessageStream.class);

    KafkaConsumer kafkaConsumer;
    Iterator messageSetIterator;
    long lastOffset = -1;
    private Semaphore commitSemaphore;

    private static MessageStream instance;

    public static MessageStream getInstance() throws Exception {
        if (instance == null) {
            throw new Exception(String.format("must run %s:init(%s kafkaConsumer) first", MessageStream.class.getName(), KafkaConsumer.class.getName()));
        }
        return instance;
    }

    public static MessageStream init(KafkaConsumer kafkaConsumer) {
        if (instance != null) {
            return instance;
        }
        instance = new MessageStream(kafkaConsumer);
        return instance;
    }

    protected MessageStream(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.commitSemaphore = new Semaphore();
    }

    public boolean hasNext() {
        return true;
    }

    public ConsumerRecord next(long timeOut) {

        long start = System.currentTimeMillis();

        if (messageSetIterator == null || !messageSetIterator.hasNext()) {
            messageSetIterator = kafkaConsumer.poll(1000).iterator();
            while (!messageSetIterator.hasNext()) {
                logger.debug("No messages returned. Sleeping for 10s.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (System.currentTimeMillis() - start > timeOut) {
                    return null;
                }
                messageSetIterator = kafkaConsumer.poll(1000).iterator();
            }
        }
        ConsumerRecord message = (ConsumerRecord) messageSetIterator.next();
        lastOffset = message.offset();
        return message;
    }

    public void commit(TopicPartition topicPartition, Long offset) {
        try {
            commitSemaphore.take();
            kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)));
        } catch (InterruptedException e) {
            logger.error(e);
        } finally {
            commitSemaphore.release();
        }
    }

    @Override
    public String toString() {
        return String.format("MessageStream(topic=%s,offset=%d)",kafkaConsumer.listTopics().toString(),lastOffset);
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