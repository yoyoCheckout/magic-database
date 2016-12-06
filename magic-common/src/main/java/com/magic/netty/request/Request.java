package com.magic.netty.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.magic.constants.Constants;

public class Request {

	@JsonIgnore
	private String guid;

	private long version;

	private long expireTime;

	private String method;

	private String key;

	private String value;

	public Request() {
	}

	public Request(String method, String key, String value, short clientIndex) {
		this(method, key, value, clientIndex, Constants.EXPIRE_TIME_DEFAULT);
	}

	public Request(String method, String key, String value, short clientIndex, long expireTime) {
		this(method, key, value, newVersion(clientIndex), expireTime);
	}

	public Request(String method, String key, String value, long version) {
		this(method, key, value, version, Constants.EXPIRE_TIME_DEFAULT);
	}

	public Request(String method, String key, String value, long version, long expireTime) {
		this.version = version;
		this.expireTime = expireTime;
		this.key = key;
		this.value = value;
		this.method = method;
		this.guid = normalGuid();
	}

	private static long newVersion(short clientIndex) {
		return System.currentTimeMillis() << Constants.TIME_SHIFT_FOR_VERSION + clientIndex;
	}

	private String normalGuid() {
		return this.key + this.method + this.version;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
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

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public String getGuid() {
		if (guid == null) {
			guid = normalGuid();
		}
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public long getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(long expireTime) {
		this.expireTime = expireTime;
	}

}
