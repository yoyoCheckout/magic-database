package com.magic.gateway.impl;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.magic.constants.Constants;
import com.magic.gateway.MagicClient;
import com.magic.gateway.exception.BaseException;
import com.magic.gateway.exception.TimeoutException;
import com.magic.gateway.options.GatewayOptions;
import com.magic.gateway.sender.SenderManager;
import com.magic.gateway.sender.ShardResponseFuture;
import com.magic.netty.request.Request;
import com.magic.netty.request.Response;
import com.magic.service.domain.MagicServiceInstance;

public class ShardClient implements MagicClient {

	private String serviceName;

	private OneClient[] serverList;

	private int size;

	private GatewayOptions opts;

	public ShardClient(String serviceName, Collection<MagicServiceInstance> instances, GatewayOptions opts) {
		this.opts = opts;
		this.serviceName = serviceName;
		this.size = instances.size();
		this.serverList = new OneClient[this.size];
		int i = 0;
		for (MagicServiceInstance serviceInstance : instances) {
			this.serverList[i] = new OneClient(serviceInstance, this.opts);
			i++;
		}
	}

	private Response sendRequestNormalMode(Request request) throws BaseException, InterruptedException {
		return sendRequest(request, new ShardResponseFuture(size / 2 + 1));
	}

	private Response sendRequestSameValueReadMode(Request request) throws BaseException, InterruptedException {
		Response response = sendRequest(request, new ShardResponseFuture(size / 2 + 1, 1));
		return response;
	}

	private Response sendRequest(Request request, ShardResponseFuture future)
			throws BaseException, InterruptedException {
		try {
			for (int i = 0; i < size; i++) {
				OneClient oc = serverList[i];
				oc.sendRequest(request, future);
			}
			Response result = future.get(opts.timeoutMilliseconds, TimeUnit.MILLISECONDS);
			if (result == null) {
				throw new TimeoutException();
			}
			return result;
		} finally {
			SenderManager.removeFuture(request.getGuid());
		}
	}

	@Override
	public String get(String key) throws BaseException, InterruptedException {
		Request request = new Request(Constants.GET, key, null, opts.clientIndex);
		Response response = sendRequestNormalMode(request);
		return response.getValue();
	}

	@Override
	public void set(String key, String value) throws BaseException, InterruptedException {
		setWithExpireTime(key, value, Constants.EXPIRE_TIME_DEFAULT);
	}

	@Override
	public void setWithExpireTime(String key, String value, long expireTime)
			throws BaseException, InterruptedException {
		if (expireTime > 0) {
			expireTime = System.currentTimeMillis() + expireTime;
		}
		Request request = new Request(Constants.SET, key, value, opts.clientIndex, expireTime);
		Response response = null;
		if (opts.checkBeforeWrite) {
			Request requestBefore = new Request(Constants.BEFORE_SET, key, value, opts.clientIndex, expireTime);
			Response responseBefore = sendRequestNormalMode(requestBefore);
			if (Constants.RESPONSE_OK.equals(responseBefore.getValue())) {
				// 可以设置
				response = sendRequestSameValueReadMode(request);
			}
		} else {
			// 可以设置
			response = sendRequestSameValueReadMode(request);
		}
		if (response != null && Constants.RESPONSE_OK.equals(response.getValue())) {
			return;
		}
		throw new BaseException();
	}

	@Override
	public void delete(String key) throws BaseException, InterruptedException {
		Request request = new Request(Constants.DELETE, key, null, opts.clientIndex);
		Response response = null;
		if (opts.checkBeforeWrite) {
			Request requestBefore = new Request(Constants.BEFORE_SET, key, null, opts.clientIndex);
			Response responseBefore = sendRequestNormalMode(requestBefore);
			if (Constants.RESPONSE_OK.equals(responseBefore.getValue())) {
				// 可以设置
				response = sendRequestSameValueReadMode(request);
			}
		} else {
			// 可以设置
			response = sendRequestSameValueReadMode(request);
		}
		if (response != null && Constants.RESPONSE_OK.equals(response.getValue())) {
			return;
		}
		throw new BaseException();
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public OneClient[] getServerList() {
		return serverList;
	}

	public void setServerList(OneClient[] serverList) {
		this.serverList = serverList;
	}

}
