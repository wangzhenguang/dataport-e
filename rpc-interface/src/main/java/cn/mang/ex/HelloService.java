package cn.mang.ex;

import cn.mang.ex.domain.Message;

public interface HelloService {

    String hello(String name);
    String hello(Message message);
}
