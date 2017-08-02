package org.l1024.kafka.archiver.config;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class Configuration {

    private static final Logger logger = Logger.getLogger(Configuration.class);

    public static Map<String, Object> loadKafkaConfiguration(String fileName) throws IOException {

        ClassLoader classLoader = Configuration.class.getClassLoader();

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File(fileName)));
        } catch (FileNotFoundException e) {
            props.load(classLoader.getResourceAsStream(fileName));
        }
        Map<String, Object> kafkaConfig = new HashMap<>();
        props.forEach((key, value) -> kafkaConfig.put(key.toString(), value));

        return kafkaConfig;
    }

    public static Configuration loadConfiguration(String fileName) throws IOException {

        ClassLoader classLoader = Configuration.class.getClassLoader();

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File(fileName)));
        } catch (FileNotFoundException e) {
            props.load(classLoader.getResourceAsStream(fileName));
        }
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
