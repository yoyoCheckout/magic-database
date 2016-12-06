package com.magic.bitcask.options;

public class BitCaskOptions {

	public int fileNumToMerge = 3; // 存储文件达到这个数量进行合并
	public int bitcaskGetMaxRetries = 2; // bitcask读数据时最大重试次数

	public long maxFileSize = 24; // 1gb file size
	public boolean readWrite = true;
	public int openTimeoutSecs = 20;

	public long closeWriteDelay = 5 * 60 * 1000; // 存储文件延迟关闭写通道（毫秒）
	public int hourToMerge = 3; // 在几点合并存储文件，比如半夜3点
	public int periodDayToMerge = 1; // 每隔几天进行一次合并存储文件

}
