package org.l1024.kafka.archiver.config;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class Configuration {

    private static final Logger logger = Logger.getLogger(Configuration.class);

    public static Map<String, Object> loadKafkaConfiguration(String fileName) throws IOException {

        File file = new File(fileName);

        logger.info(String.format("Loading kafka-server config (%s)", file.getAbsolutePath()));

        Properties props = new Properties();
        props.load(new FileInputStream(file));
        Map<String, Object> kafkaConfig = new HashMap<>();
        props.forEach((key, value) -> kafkaConfig.put(key.toString(), value));

        return kafkaConfig;
    }

    public static Configuration loadConfiguration(String fileName) throws IOException {

        File file = new File(fileName);

        logger.info(String.format("Loading kafka-archiver config (%s)", file.getAbsolutePath()));

        Properties props = new Properties();
        props.load(new FileInputStream(fileName));
        return new PropertyConfiguration(props);
    }

    public abstract String getS3AccessKey();
    public abstract String getS3SecretKey();
    public abstract String getS3Bucket();
    public abstract String getS3Prefix();

    public abstract Set<String> getTopics();

    public abstract int getMinTotalMessageSizePerChunk();

    public abstract int getMinTotalMessageCountPerChunk();

    public abstract int getMaxCommitInterval();
}
