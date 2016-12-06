package com.magic.gateway.options;

import com.magic.service.domain.AllServiceInstance;

public class GatewayOptions {

	public short clientIndex = 0;

	public String bitcaskDir = "/Users/yinwenhao/workspace/magic-database/magic-bitcask"; // 数据存储目录
	public String zkBasePath = "/magic/database";

	public String zkList = "10.128.8.57:2181";

	public long timeoutMilliseconds = 3 * 1000; // 等待bitcask服务端响应的超时毫秒数

	public int threadNumForOneChannel = 1;

	public long periodMilliSeconds = 30000; // 扫描zk获取服务列表的时间间隔（毫秒）

	public AllServiceInstance allServiceInstance = null; // 配置文件中所有服务的信息

	public long periodMilliSecondsForReconnect = 1000; // netty重连的重试时间间隔

	public boolean checkBeforeWrite = false; // 写入前是否先检查服务器状态。开启的话会让性能降低，但脏读的可能会几乎降为0

}
