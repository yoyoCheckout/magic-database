package com.magic.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.magic.netty.handler.DecodeHandler;
import com.magic.netty.handler.EncodeHandler;
import com.magic.netty.handler.MyCloseHandler;
import com.magic.netty.handler.TcpServerHandler;
import com.magic.netty.request.Request;
import com.magic.netty.serial.InputFactory;
import com.magic.netty.serial.OutputFactory;
import com.magic.netty.serial.impl.JsonSerialFactory;
import com.magic.server.NetServer;
import com.magic.server.options.ServerOptions;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

public class NettyServer implements NetServer {

	private Logger log = LoggerFactory.getLogger(getClass());

	private InputFactory input = new JsonSerialFactory();

	private OutputFactory output = new JsonSerialFactory();

	private int bossNum = 1;

	private int workerNum = 0;

	private ServerBootstrap b;

	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;

	private ChannelFuture future;

	private ServerOptions opts;

	public NettyServer(ServerOptions opts) {
		this.opts = opts;
		this.bossGroup = new NioEventLoopGroup(bossNum);
		this.workerGroup = new NioEventLoopGroup(workerNum);
	}

	@Override
	public void shutdown() {
		log.info("NettyServer shutting down gracefully...");
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
	}

	@Override
	public void startServer() throws Exception {
		try {
			b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
					.handler(new LoggingHandler(LogLevel.INFO)).childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline p = ch.pipeline();
							p.addLast("idleStateHandler", new IdleStateHandler(MyTcpConstants.timeForIdle, 0, 0));
							p.addLast("closeHandler", new MyCloseHandler());
							p.addLast("lengthFieldBasedFrameDecoder", new LengthFieldBasedFrameDecoder(
									MyTcpConstants.maxFrameLength, 0, MyTcpConstants.lengthFieldLength));
							p.addLast("lengthFieldPrepender",
									new LengthFieldPrepender(MyTcpConstants.lengthFieldLength));
							p.addLast("encoder", new EncodeHandler(output));
							p.addLast("decoder", new DecodeHandler(input, Request.class));
							p.addLast("actionHandler", new TcpServerHandler());
						}
					});

			// 使用对象池
			b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
			b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);// 关键是这句

			// Bind and start to accept incoming connections.
			this.future = b.bind(opts.port).sync();
		} catch (Exception e) {
			log.error("netty server start error", e);
			this.shutdown();
			throw e;
		}
	}

	public InputFactory getInput() {
		return input;
	}

	public void setInput(InputFactory input) {
		this.input = input;
	}

	public OutputFactory getOutput() {
		return output;
	}

	public void setOutput(OutputFactory output) {
		this.output = output;
	}

	public int getBossNum() {
		return bossNum;
	}

	public void setBossNum(int bossNum) {
		this.bossNum = bossNum;
	}

	public int getWorkerNum() {
		return workerNum;
	}

	public void setWorkerNum(int workerNum) {
		this.workerNum = workerNum;
	}

	@Override
	public void sync() throws Exception {
		this.future.channel().closeFuture().sync();
	}

}
