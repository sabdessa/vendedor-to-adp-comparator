package com.kramp;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class FileChunkConsumer implements Runnable {


    private final MappedByteBuffer mappedByteBuffer;
    private BiConsumer<String, String> consumer;


    FileChunkConsumer(MappedByteBuffer mappedByteBuffer, BiConsumer<String, String> consumer) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.consumer = consumer;
    }

    @Override
    public void run() {

        StringBuilder line = new StringBuilder();
        while (mappedByteBuffer.hasRemaining()) {
            char c = (char) mappedByteBuffer.get();
            if (c == '\n') {
                int indexSeparator = line.indexOf(",");
                String adpId = line.substring(0, indexSeparator);
                String rowHash = line.substring(indexSeparator + 1);
                consumer.accept(adpId, rowHash);
                line = new StringBuilder();
            } else {
                line.append(c);
            }
        }
    }
}
