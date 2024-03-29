package com.magic.constants;

public class Constants {

	/**
	 * 协议版本号
	 */
	public static final int PROTOCOL_VERSION = 0;

	/**
	 * magic database server name head
	 */
	public static final String SERVICE_NAME_HEAD = "md-server#";

	public static final String GET = "get";

	public static final String SET = "set";

	public static final String BEFORE_SET = "before_set";

	public static final String RESPONSE_OK = "ok";

	public static final String DELETE = "delete";

	public static final String GOSSIP = "gossip";

	public static final String GOSSIP_REQUIRE = "gossip_require";

	public static final String GOSSIP_SET = "gossip_set";

	/**
	 * 时间戳（毫秒）左移10位，加上分片id，组成version
	 */
	public static final int TIME_SHIFT_FOR_VERSION = 10;

	/**
	 * 过期时间——默认0，表示不限制
	 */
	public static final long EXPIRE_TIME_DEFAULT = 0;

}
