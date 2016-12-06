package com.magic.netty.dispatcher;

import com.magic.netty.request.Request;
import com.magic.netty.request.Response;

public interface Dispatcher {

	public Response getResult(Request request) throws Exception;

}
