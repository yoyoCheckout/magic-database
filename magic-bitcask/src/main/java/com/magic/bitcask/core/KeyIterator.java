package com.magic.bitcask.core;

public interface KeyIterator {

	public void each(String key, long version, long expireTime, long position, int size, int crc32) throws Exception;

}
