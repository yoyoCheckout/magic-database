package com.magic.gateway.impl;

import java.util.Map;

import com.magic.constants.Constants;
import com.magic.gateway.ConsistentHash;
import com.magic.gateway.MagicClient;
import com.magic.gateway.exception.BaseException;
import com.magic.gateway.exception.NoServiceException;

public class ShardClientManager implements MagicClient {

	private ConsistentHash consistentHash = new ConsistentHashImpl();

	private Map<Integer, ShardClient> shardClientMap;

	private int shardNum;

	public ShardClientManager(Map<Integer, ShardClient> shardClientMap) {
		this.shardClientMap = shardClientMap;
		this.shardNum = this.shardClientMap.size();
	}

	private int getShardIndex(String key) throws NoServiceException {
		if (shardNum <= 0) {
			throw new NoServiceException();
		}
		return consistentHash.getShardIndex(key, shardNum);
	}

	@Override
	public String get(String key) throws BaseException, InterruptedException {
		int serviceIndex = getShardIndex(key);
		return shardClientMap.get(serviceIndex).get(key);
	}

	@Override
	public void set(String key, String value) throws BaseException, InterruptedException {
		setWithExpireTime(key, value, Constants.EXPIRE_TIME_DEFAULT);
	}

	@Override
	public void setWithExpireTime(String key, String value, long expireTime)
			throws BaseException, InterruptedException {
		int serviceIndex = getShardIndex(key);
		shardClientMap.get(serviceIndex).setWithExpireTime(key, value, expireTime);
	}

	@Override
	public void delete(String key) throws BaseException, InterruptedException {
		int serviceIndex = getShardIndex(key);
		shardClientMap.get(serviceIndex).delete(key);
	}

}
