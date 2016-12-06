package com.magic.netty.listener;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionListener implements GenericFutureListener<Future<Void>> {

	public final static ExceptionListener TRACE = new ExceptionListener();

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void operationComplete(Future<Void> future) throws Exception {
		if (!future.isSuccess() && future.cause() != null) {
//			future.cause().printStackTrace();
			log.error(future.cause().getMessage(), future.cause());
		}
	}
}
