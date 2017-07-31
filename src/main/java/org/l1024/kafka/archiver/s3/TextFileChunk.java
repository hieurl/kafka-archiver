package org.l1024.kafka.archiver.s3;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TextFileChunk extends Chunk {

    public static Chunk createChunk(String topic) throws IOException {
        return new TextFileChunk(topic);
    }

    protected TextFileChunk(String topic) throws IOException {
        super(topic);
        tmpFile = Files.createTempFile("s3sink", null);
    }

    private Path tmpFile;

    @Override
    public void appendMessageToChunk(ConsumerRecord messageAndOffset) throws IOException {
        int messageSize = messageAndOffset.serializedValueSize();
        totalMessageCount += 1;
        totalMessageSize += messageSize;
        endOffset = messageAndOffset.offset();

        Files.write(tmpFile,(messageAndOffset.value().toString()+"\n").getBytes(), StandardOpenOption.APPEND);
    }

    @Override
    public File finalizeChunk() throws IOException {
        return tmpFile.toFile();
    }

    @Override
    public void cleanUp() {
        tmpFile.toFile().delete();
    }
}
