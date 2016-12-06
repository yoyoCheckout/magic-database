package com.magic.netty.serial;

import java.io.IOException;

public interface InputFactory {
	
	public <T> T input(byte[] data, Class<T> clazz) throws IOException;

	public <T> T input(byte[] data, int offest, Class<T> clazz) throws IOException;
	
}
