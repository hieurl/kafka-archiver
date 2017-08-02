package org.l1024.kafka.archiver.s3;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SequenceFileChunk extends Chunk {

    public static Chunk createChunk(String topic) throws IOException {
        return new SequenceFileChunk(topic);
    }

    private File tmpFile;
    private OutputStream tmpOutputStream;
    private SequenceFile.Writer tmpWriter;

    protected Text key = new Text();
    protected BytesWritable value = new BytesWritable();

    protected SequenceFileChunk(String topic) throws IOException {
        super(topic);
        tmpFile = File.createTempFile("s3sink", null);
        tmpOutputStream = new FileOutputStream(tmpFile);
        S3Sink.logger.debug("Created tmpFile: " + tmpFile);
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration(false);
        tmpWriter = SequenceFile.createWriter(
                hadoopConf,
                new FSDataOutputStream(
                        tmpOutputStream,
                        new FileSystem.Statistics("")),
                Text.class,
                BytesWritable.class,
                SequenceFile.CompressionType.BLOCK,
                new BZip2Codec()
        );
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

    @Override
    public void appendMessageToChunk(ConsumerRecord messageAndOffset) throws IOException {
        key.set(String.format("%s:%d", topicPartition, messageAndOffset.offset()));

        int messageSize = messageAndOffset.serializedValueSize();
        totalMessageCount += 1;
        totalMessageSize += messageSize;
        endOffset = messageAndOffset.offset();

        value.set(messageAndOffset.value().toString().getBytes(), 0, messageSize);
        tmpWriter.append(key, value);
    }

    @Override
    public File finalizeChunk() throws IOException {
        tmpWriter.close();
        tmpOutputStream.close();
        return tmpFile;
    }

    public void cleanUp() {
        tmpFile.delete();
    }

    public boolean isEmpty() {
        return totalMessageCount == 0;
    }
}
