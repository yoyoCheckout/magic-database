package com.magic.netty.handler;

import com.magic.netty.listener.ExceptionListener;
import com.magic.netty.serial.OutputFactory;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * 编码
 * 
 * @author yinwenhao
 *
 */
@Sharable
public class EncodeHandler extends ChannelOutboundHandlerAdapter {

	private final PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

	private final OutputFactory output;

	public EncodeHandler(OutputFactory output) {
		this.output = output;
	}

	@Override
	public void write(ChannelHandlerContext ctx, final Object msg, ChannelPromise promise) throws Exception {
		this.writeAndFlush(ctx, this.output.output(msg));
	}

	private void writeAndFlush(ChannelHandlerContext ctx, byte[] serial) {
		ctx.writeAndFlush(
				this.allocator.heapBuffer(serial.length + 1).writeByte(output.getProtocolVersion()).writeBytes(serial))
				.addListener(ExceptionListener.TRACE);
	}

}
