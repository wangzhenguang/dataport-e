package cn.mang.ex.sample.client;

import cn.mang.ex.HelloService;
import cn.mang.ex.client.RpcProxy;
import cn.mang.ex.domain.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class HelloServiceTest {
    @Autowired
    private RpcProxy proxy;

//    public static void main(String[] args){
//        new ClassPathXmlApplicationContext("spring.xml");
//
//    }


    @Test
    public void helloTest() {
        HelloService helloService = proxy.create(HelloService.class);
        Object hello = helloService.hello("hello");
        System.out.println("服务端返回结果 " + hello);
    }

    @Test
    public void helloMsgTest() {
        HelloService helloService = proxy.create(HelloService.class);
        Message message = new Message("发送msg");
        String hello = helloService.hello(message);
        System.out.println("服务端返回结果 " + hello);
    }

}
