package com.magic.netty.serial.impl;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonAutoCloseOutput implements Closeable {

	private final static int BUFFER_SIZE = 1024;

	public static final ObjectMapper MAPPER = new ObjectMapper();

	private final ByteArrayOutputStream arrays = new ByteArrayOutputStream(BUFFER_SIZE);

	public JsonAutoCloseOutput writeObject(Object ob) throws IOException {
		MAPPER.writeValue(this.arrays, ob);
		return this;
	}

	public byte[] toByteArray() {
		return this.arrays.toByteArray();
	}

	@Override
	public void close() throws IOException {
		this.arrays.close();
	}
}
