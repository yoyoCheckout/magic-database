package com.magic.netty.serial.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.magic.netty.serial.InputFactory;
import com.magic.netty.serial.OutputFactory;

/**
 * @author when_how 2015年11月24日
 */
public class JsonSerialFactory implements OutputFactory, InputFactory {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final byte protocolVersion = 0;

	@Override
	public <T> T input(byte[] data, Class<T> clazz) throws IOException {
		return input(data, 0, data.length, clazz);
	}

	@Override
	public <T> T input(byte[] data, int offset, Class<T> clazz) throws IOException {
		return input(data, offset, data.length - offset, clazz);
	}

	@Override
	public byte[] output(Object data) throws IOException {
		try (JsonAutoCloseOutput output = new JsonAutoCloseOutput()) {
			byte[] result = output.writeObject(data).toByteArray();
			if (log.isDebugEnabled()) {
				log.debug("json output: " + new String(result));
			}
			return result;
		}
	}

	private <T> T input(byte[] data, int offset, int length, Class<T> clazz) throws IOException {
		if (log.isDebugEnabled()) {
			// log.debug("json input: " + new String(data));
			log.debug("json input with offset: " + new String(data, offset, length));
		}
		try {
			return JsonAutoCloseOutput.MAPPER.readValue(data, offset, length, clazz);
		} catch (IOException e) {
			log.error("json serialize error. ", e);
			throw e;
		}
	}

	@Override
	public byte getProtocolVersion() {
		return protocolVersion;
	}

}
