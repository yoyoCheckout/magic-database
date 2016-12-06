package com.magic.netty;

public class MyTcpConstants {

	/** 检测时间（超过这个时间没有读到数据，则断开连接，秒） */
	public static final int timeForIdle = 3600 * 2;

	/** tcp包内容的长度（字节） */
	public static final int lengthFieldLength = 2;
	public static final int maxFrameLength = Short.MAX_VALUE;

	/** tcp协议版本号的长度（字节） */
	public static final int protocolVersionLength = 1;

	/** tcp包头部的长度（字节） */
	public static final int headLength = lengthFieldLength + protocolVersionLength;

}
