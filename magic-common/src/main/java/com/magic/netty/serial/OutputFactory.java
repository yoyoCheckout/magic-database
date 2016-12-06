package com.magic.netty.serial;

import java.io.IOException;

public interface OutputFactory {

	public byte[] output(Object data) throws IOException;

	public byte getProtocolVersion();

}
