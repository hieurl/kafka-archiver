package org.l1024.kafka.archiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.l1024.kafka.archiver.config.Configuration;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;


public class Archiver implements ArchiverMBean {

    private static final Logger logger = Logger.getLogger(Archiver.class);

    private Map<String, Object> kafkaConfig;
    private Configuration configuration;

    private ThreadContainer threadContainer;

    public Archiver(Map<String, Object> kafkaConfig, Configuration configuration) {
        this.kafkaConfig = kafkaConfig;
        this.configuration = configuration;
    }

    public void start() throws IOException, InterruptedException {
        SinkFactory sinkFactory =
                new SinkFactory(
                        configuration.getS3AccessKey(),
                        configuration.getS3SecretKey(),
                        configuration.getS3Bucket(),
                        configuration.getS3Prefix()
                );
        threadContainer = new ThreadContainer(
                kafkaConfig,
                new ArrayList<>(getTopics()),
                sinkFactory,
                configuration.getMaxCommitInterval(),
                configuration.getMinTotalMessageCountPerChunk(),
                configuration.getMinTotalMessageSizePerChunk()
        );
        threadContainer.start();
    }

    private static class ThreadContainer {

        private List<String> topics;

        private ArchivingWorker worker;
        private Thread thread;

        private Map<String, Object> kafkaConfig;
        private SinkFactory sinkFactory;
        private int maxCommitInterval;
        private int maxMessageCountPerChunk;
        private int maxChunkSize;

        public ThreadContainer(Map<String, Object> kafkaConfig,
                               List<String> topics,
                               SinkFactory sinkFactory,
                               Integer maxCommitInterval,
                               Integer maxMessageCountPerChunk,
                               Integer maxChunkSize
                               ) {
            this.topics = topics;
            this.kafkaConfig = kafkaConfig;
            this.sinkFactory = sinkFactory;
            this.maxCommitInterval = maxCommitInterval;
            this.maxMessageCountPerChunk = maxMessageCountPerChunk;
            this.maxChunkSize = maxChunkSize;
        }

        public void start() throws IOException, InterruptedException {
            logger.info(String.format("Creating worker. (%s)", topics.toString()));
            createThread();
            logger.debug(worker);

            while (true) {
                maintain();
                Thread.sleep(60000);
            }
        }

        public boolean maintain() throws IOException {
            boolean restart = !thread.isAlive() && !worker.isFinished();
            if (restart) {
                logger.info(String.format("Thread (%s) died. Recreating worker. (%s)", thread, topics.toString()));
                createThread();
            }

            worker.cron();
            logger.debug(worker);
            return restart;
        }

        private void createThread() {
            KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaConfig);
            kafkaConsumer.subscribe(this.topics);
            worker = new ArchivingWorker(
                    MessageStream.init(kafkaConsumer),
                    sinkFactory,
                    maxMessageCountPerChunk,
                    maxChunkSize,
                    maxCommitInterval
                    );
            thread = new Thread(worker);
            thread.setUncaughtExceptionHandler(new ExceptionHandler());
            thread.start();
        }

        @Override
        public String toString() {
            return String.format("ThreadContainer(topic=%s,worker=%s)", topics.toString(), worker);
        }

        private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("ThreadContainer: ", e);
            }
        }

    }

    private static class ArchivingWorker implements Runnable {

        private SinkFactory sinkFactory;
        int maxMessageCountPerChunk;
        int maxChunkSize;
        int maxCommitInterval;
        private boolean finished = false;

        private Map<String, Sink> sinkMap = new HashMap<>();
        private MessageStream messageStream;

        private ArchivingWorker(
                MessageStream messageStream,
                SinkFactory sinkFactory,
                int maxMessageCountPerChunk,
                int maxChunkSize,
                int maxCommitInterval
        ) {
            this.sinkFactory = sinkFactory;
            this.maxMessageCountPerChunk = maxMessageCountPerChunk;
            this.maxCommitInterval = maxCommitInterval;
            this.maxChunkSize = maxChunkSize;
            this.messageStream = messageStream;
        }

        @Override
        public void run() {

            try {

                messageStream.kafkaConsumer.subscription().forEach(topic -> {
                    try {
                        Sink sink = sinkFactory.createSink(topic.toString(), maxMessageCountPerChunk, maxChunkSize, maxCommitInterval);
                        sinkMap.put(topic.toString(), sink);
                        logger.info(String.format("Worker starts archiving messageStream from topic %s starting with offset %d", topic, sink.getMaxCommittedOffset()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });


                while (messageStream.hasNext()) {
                    ConsumerRecord consumerRecord = messageStream.next(maxCommitInterval);
                    if (consumerRecord != null) {
                        Sink sink = sinkMap.get(consumerRecord.topic());
                        if (sink.append(consumerRecord, true)) {
                            sink.getCommittedOffsets().forEach((p, offset) ->
                                    messageStream.commit(new TopicPartition(consumerRecord.topic(), p), offset+1));
                            sink.postCommitFinished();
                        }
                    }
                }

                finished = true;

                logger.info(toString() + " finished.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isFinished() {
            return finished;
        }

        public void cron() {
            sinkMap.forEach((s, sink) -> {
                try {
                    if (sink.maybeCommitChunk(true)) {
                        sink.getCommittedOffsets().forEach((p, offset) ->
                                messageStream.commit(new TopicPartition(s, p), offset+1));
                    }
                    sink.postCommitFinished();
                } catch (IOException e) {
                    logger.error(e);
                }
            });
        }

        @Override
        public String toString() {
            if (messageStream == null || sinkMap == null) {
                return String.format("ArchivingWorker(uninitialized)");
            } else {
                return String.format("ArchivingWorker(messageStream=%s,sink=%s)", messageStream.toString(), sinkMap.toString());
            }
        }
    }


    @Override
    public String toString() {
        return String.format("Archiver(threadContainer=%s)", threadContainer);
    }

    // JMX MBean methods
    @Override
    public Set<String> getTopics() {
        return configuration.getTopics();
    }

    public static void main(String[] args) throws IOException, InterruptedException, MalformedObjectNameException, MBeanRegistrationException, InstanceAlreadyExistsException, NotCompliantMBeanException {

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        /*
        if (args.length != 2) {
            System.out.println("Usage: java -jar <kafka consumer jar> <server properties> <archiver properties>");
        }
        */
        Map<String, Object> kafkaConfig = Configuration.loadKafkaConfiguration("src/main/resources/kafkaConfig.properties");
        Configuration configuration = Configuration.loadConfiguration("src/main/resources/serverConfig.properties");

        Archiver archiver = new Archiver(kafkaConfig, configuration);

        mbs.registerMBean(archiver, new ObjectName("Archiver:type=org.l1024.kafka.archiver.Archiver"));

        archiver.start();
    }

}

