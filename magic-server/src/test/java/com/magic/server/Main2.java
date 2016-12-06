package com.magic.server;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import com.magic.server.Server;
import com.magic.server.impl.ServerImpl;
import com.magic.server.options.ServerOptions;
import com.magic.service.domain.AllServiceInstance;
import com.magic.util.Json;

public final class Main2 {

	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configureAndWatch(System.getProperty("confPath") + "/log4j.properties", 60 * 1000);
		ServerOptions opts = new ServerOptions();
		opts.shardIndex = 0;
		opts.port = 7867;
		opts.bitcaskDir = "/Users/yinwenhao/workspace/magic-database/.test-data/magic-bitcask-0-2";

		File jsonFile = new File(System.getProperty("confPath") + "/services.json");
		opts.allServiceInstance = Json.readValue(jsonFile, AllServiceInstance.class);
		opts.allServiceInstance.getSelfInstance().setPort(opts.port);

		Server serverImpl = new ServerImpl(opts);
		serverImpl.start();
	}

}
