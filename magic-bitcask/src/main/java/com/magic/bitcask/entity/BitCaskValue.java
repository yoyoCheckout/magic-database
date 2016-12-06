package com.magic.bitcask.entity;

public class BitCaskValue {

	private long version;

	private long expireTime;

	private String key;

	private String value;

	public BitCaskValue() {
	}

	public BitCaskValue(String key) {
		this.version = 0;
		this.expireTime = 0;
		this.key = key;
		this.value = null;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(long expireTime) {
		this.expireTime = expireTime;
	}

}
