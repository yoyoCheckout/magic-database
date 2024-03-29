package com.magic.bitcask.entity;

import com.magic.bitcask.constants.Constants;

public class BitCaskKey {

	private int fileId;

	private long version;

	private long expireTime;

	private long position;

	private int size;

	private int crc32;

	public BitCaskKey(int fileId, long version, long expireTime, long position, int size, int crc32) {
		this.fileId = fileId;
		this.version = version;
		this.expireTime = expireTime;
		this.position = position;
		this.size = size;
		this.crc32 = crc32;
	}

	public boolean isNewerThan(BitCaskKey old) {
		return old.getVersion() < this.getVersion();
	}

	/**
	 * 插入数据时的时间，毫秒
	 * 
	 * @return
	 */
	public long getCreateTime() {
		return version >> Constants.TIME_SHIFT_FOR_VERSION;
	}

	public int getFileId() {
		return fileId;
	}

	public void setFileId(int fileId) {
		this.fileId = fileId;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getCrc32() {
		return crc32;
	}

	public void setCrc32(int crc32) {
		this.crc32 = crc32;
	}

	public long getVersion() {
		return version;
	}

	public long getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(long expireTime) {
		this.expireTime = expireTime;
	}

}
