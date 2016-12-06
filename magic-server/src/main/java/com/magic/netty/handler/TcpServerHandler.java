package com.magic.netty.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.magic.executor.Executor;
import com.magic.executor.factory.ExecutorFactory;
import com.magic.netty.dispatcher.task.RequestTask;
import com.magic.netty.request.Request;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class TcpServerHandler extends ChannelInboundHandlerAdapter {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private Executor executor = ExecutorFactory.createRequestExecutorAndInit();

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Request request = (Request) msg;
		if (log.isDebugEnabled()) {
			log.debug("recieve Request. guid:{} version:{} method:{} key:{} value:{}", request.getGuid(),
					request.getVersion(), request.getMethod(), request.getKey(), request.getValue());
		}
		RequestTask task = new RequestTask(request, ctx);
		executor.execute(task);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		log.error("exceptionCaught", cause);
		ctx.close();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) {
		log.debug("连接上了");
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) {
		log.debug("断开连接了");
	}
}
