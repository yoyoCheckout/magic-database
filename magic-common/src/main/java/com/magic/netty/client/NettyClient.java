package com.magic.netty.client;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.magic.netty.MyTcpConstants;
import com.magic.netty.handler.DecodeHandler;
import com.magic.netty.handler.EncodeHandler;
import com.magic.netty.request.Response;
import com.magic.netty.serial.impl.JsonSerialFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class NettyClient {

	private Logger log = LoggerFactory.getLogger(getClass());

	private EventLoopGroup group;

	private Bootstrap b;

	private ChannelFuture f;

	private ChannelHandler ch;

	private boolean closed = false;

	private String address;
	private int port;

	private long periodMilliSecondsForReconnect;

	public NettyClient(ChannelInboundHandlerAdapter requestHandler, String address, int port,
			int threadNumForOneChannel, long periodMilliSecondsForReconnect) {
		this.periodMilliSecondsForReconnect = periodMilliSecondsForReconnect;
		this.address = address;
		this.port = port;
		JsonSerialFactory serialFactory = new JsonSerialFactory();
		EncodeHandler encodeHandler = new EncodeHandler(serialFactory);
		DecodeHandler decodeHandler = new DecodeHandler(serialFactory, Response.class);

		ChannelInboundHandler reconnector = new ReconnectHandler();

		this.group = new NioEventLoopGroup(threadNumForOneChannel);

		this.b = new Bootstrap();

		this.ch = new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addFirst(reconnector);
				p.addLast("lengthFieldBasedFrameDecoder", new LengthFieldBasedFrameDecoder(
						MyTcpConstants.maxFrameLength, 0, MyTcpConstants.lengthFieldLength));
				p.addLast("lengthFieldPrepender", new LengthFieldPrepender(MyTcpConstants.lengthFieldLength));
				p.addLast("encoder", encodeHandler);
				p.addLast("decoder", decodeHandler);
				p.addLast("requestHandler", requestHandler);
			}
		};

		this.b.group(this.group).channel(NioSocketChannel.class).handler(this.ch);
	}

	@Sharable
	private class ReconnectHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			super.channelInactive(ctx);
			scheduleDoConnect(ctx.channel());
		}
	}

	public void startClient() {
		try {
			// Start the client.
			doConnect();
			f.sync();
			logStartSuccess();
		} catch (Exception e) {
			logStartFailed(e);
		}
	}

	private void logStartSuccess() {
		log.info("Netty client started. remote[{}:{}]", address, port);
	}

	private void logStartFailed(Throwable t) {
		log.error("Netty client started failed. remote[" + address + ":" + port + "]", t);
	}

	private void scheduleDoConnect(Channel channel) {
		channel.eventLoop().schedule(() -> doConnect(), periodMilliSecondsForReconnect, TimeUnit.MILLISECONDS);
	}

	private void doConnect() {
		if (closed) {
			return;
		}

		f = b.connect(new InetSocketAddress(address, port));

		f.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture f) throws Exception {
				if (f.isSuccess()) {
					logStartSuccess();
				} else {
					logStartFailed(f.cause());
					scheduleDoConnect(f.channel());
				}
			}
		});
	}

	public void shutdown() {
		log.info("NettyClient shutting down gracefully...");
		closed = true;
		group.shutdownGracefully();
	}

}
