package com.itcast.kafkademo.chapter2;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Company company) {
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        if (data == null) return null;
        byte[] name, address;
        try {
            name = data.getName() != null ? data.getName().getBytes(StandardCharsets.UTF_8) : new byte[0];
            address = data.getAddress() != null ? data.getAddress().getBytes(StandardCharsets.UTF_8) : new byte[0];
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }

}
