package com.magic.netty.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.magic.netty.MyTcpConstants;
import com.magic.netty.serial.InputFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandler.Sharable;

/**
 * 解码
 * 
 * @author yinwenhao
 *
 */
@Sharable
public class DecodeHandler extends ChannelInboundHandlerAdapter {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final InputFactory input;

	private final Class<?> clazz;

	public DecodeHandler(InputFactory input, Class<?> clazz) {
		this.input = input;
		this.clazz = clazz;
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, Object message) throws Exception {
		ByteBuf buffer = ByteBuf.class.cast(message);
		short length = buffer.readShort();
		int protocolVersion = buffer.readByte();
		if (log.isDebugEnabled()) {
			log.debug("recieve Request. length:" + length + ", protocolVersion:" + protocolVersion);
		}
		byte[] bytes;
		if (buffer.hasArray()) {
			bytes = buffer.array();
		} else {
			bytes = new byte[buffer.capacity()];
			buffer.getBytes(0, bytes);
		}
		buffer.release();
		ctx.fireChannelRead(input.input(bytes, MyTcpConstants.headLength, this.clazz));
	}

}
