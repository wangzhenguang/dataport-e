package cn.mang.ex.server;

import cn.mang.ex.HelloService;
import cn.mang.ex.domain.Message;


@RpcService(HelloService.class)
public class HelloServiceImpl implements HelloService {
    public String hello(String name) {
        return "hello rpc";
    }

    public String hello(Message message) {
        return message.getMsg();
    }
}
