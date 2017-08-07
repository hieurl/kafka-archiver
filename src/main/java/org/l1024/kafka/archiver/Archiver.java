package org.l1024.kafka.archiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.l1024.kafka.archiver.config.Configuration;

import javax.management.*;
import java.io.IOException;
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
            return String.format("ThreadContainer(topicPartition=%s,worker=%s)", topics.toString(), worker);
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
        private long lastCron;

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
            this.lastCron = new Date().getTime();
        }

        @Override
        public void run() {
            String sinkMapKey;
            try {
                while (messageStream.hasNext()) {
                    ConsumerRecord consumerRecord = messageStream.next(maxCommitInterval);
                    if (consumerRecord != null) {
                        sinkMapKey = String.format("%s-%d", consumerRecord.topic(), consumerRecord.partition());

                        if (! sinkMap.containsKey(sinkMapKey)) {
                            sinkMap.put(sinkMapKey, sinkFactory.createSink(
                                    new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                    maxMessageCountPerChunk,
                                    maxChunkSize,
                                    maxCommitInterval
                            ));
                        }
                        Sink sink = sinkMap.get(sinkMapKey);
                        if (sink.append(consumerRecord, true)) {
                            messageStream.commit(sink.getPartition(), sink.getCommittedOffset()+1);
                            logger.info(String.format("Kafka committed at %s-%d: %s", sink.getPartition().topic(), sink.getPartition().partition(), sink.getCommittedOffset()+1));
                            sink.postCommitFinished();
                        }
                    }
                    this.cron();
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
            if (new Date().getTime() - lastCron > 60000) {
                sinkMap.forEach((s, sink) -> {
                    try {
                        if (sink.maybeCommitChunk(true)) {
                            messageStream.commit(sink.getPartition(), sink.getCommittedOffset() + 1);
                            logger.info(String.format("Kafka committed at %s-%d: %s", sink.getPartition().topic(), sink.getPartition().partition(), sink.getCommittedOffset() + 1));
                        }
                        sink.postCommitFinished();
                    } catch (IOException e) {
                        logger.error(e);
                    }
                });
                lastCron = new Date().getTime();
            }
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

        //MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        Map<String, Object> kafkaConfig = Configuration.loadKafkaConfiguration("kafkaConfig.properties");
        Configuration configuration = Configuration.loadConfiguration("serverConfig.properties");

        if (args.length == 2) {
            Map<String, Object> kafkaCustomConfig = Configuration.loadKafkaConfiguration(args[0]);
            configuration = Configuration.loadConfiguration(args[1]);

            kafkaCustomConfig.forEach(kafkaConfig::put);
        }

        Archiver archiver = new Archiver(kafkaConfig, configuration);

        //mbs.registerMBean(archiver, new ObjectName("Archiver:type=org.l1024.kafka.archiver.Archiver"));

        archiver.start();
    }

}

