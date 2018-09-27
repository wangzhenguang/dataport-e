package cn.mang.ex.server;

import cn.mang.ex.RpcDecoder;
import cn.mang.ex.RpcEncoder;
import cn.mang.ex.RpcRequest;
import cn.mang.ex.RpcResponse;
import cn.mang.ex.registry.ServiceRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.HashMap;
import java.util.Map;

public class RpcServer implements ApplicationContextAware, InitializingBean {

    private static final Logger logger = LoggerFactory
            .getLogger(RpcServer.class);

    private String serverAddress;
    private ServiceRegistry serviceRegistry;

    //用于存储业务接口和实现类的实例对象(由spring所构造)
    private Map<String, Object> handlerMap = new HashMap<String, Object>();

    public RpcServer(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    //服务器绑定的地址和端口由spring在构造本类时从配置文件中传入
    public RpcServer(String serverAddress, ServiceRegistry serviceRegistry) {
        this.serverAddress = serverAddress;
        //用于向zookeeper注册名称服务的工具类
        this.serviceRegistry = serviceRegistry;
    }


    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        System.out.println("setApplicationContext");
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(RpcService.class);
        if (MapUtils.isNotEmpty(beans)) {
            for (Object o : beans.values()) {
                String name = o.getClass().getAnnotation(RpcService.class).value().getName();
                handlerMap.put(name, o);
            }
        }



    }

    /**
     * 启动netty
     *
     * @throws Exception
     */
    public void afterPropertiesSet() throws Exception {
        System.out.println("afterPropertiesSet");
        EventLoopGroup parentGroup = new NioEventLoopGroup();
        EventLoopGroup childGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(parentGroup,childGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline()
                                    .addLast(new RpcDecoder(RpcRequest.class))// 注册解码 IN-1
                                    .addLast(new RpcEncoder(RpcResponse.class))// 注册编码 OUT
                                    .addLast(new RpcHandler(handlerMap));//注册RpcHandler IN-2
                        }
                    }).option(ChannelOption.SO_BACKLOG,128)
                    .childOption(ChannelOption.SO_KEEPALIVE,true);

            String[] split = serverAddress.split(":");
            String host = split[0];
            int port = Integer.parseInt(split[1]);

            ChannelFuture sync = bootstrap.bind(host, port).sync();

            if(serviceRegistry!= null){
                serviceRegistry.register(serverAddress); //注册到zookeeper
            }
            logger.debug("启动 rpcserver");
            System.out.println("启动 rpcserver");
            sync.channel().closeFuture().sync();

        } catch (Exception e) {

        }finally {
            parentGroup.shutdownGracefully();
            childGroup.shutdownGracefully();
        }

    }
}
