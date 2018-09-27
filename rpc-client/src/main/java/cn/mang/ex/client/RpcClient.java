package cn.mang.ex.client;

import cn.mang.ex.RpcDecoder;
import cn.mang.ex.RpcEncoder;
import cn.mang.ex.RpcRequest;
import cn.mang.ex.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient extends SimpleChannelInboundHandler<RpcResponse> {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(RpcClient.class);

    private String host;
    private int port;

    private RpcResponse response;

    private final Object obj = new Object();

    public RpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }


    public RpcResponse send(RpcRequest request) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new RpcEncoder(RpcRequest.class))
                                    .addLast(new RpcDecoder(RpcResponse.class))
                                    .addLast(RpcClient.this);
                        }
                    }).option(ChannelOption.SO_KEEPALIVE, true);


            ChannelFuture future = bootstrap.connect(host, port).sync();
            future.channel().writeAndFlush(request).sync();

            synchronized (obj) {    // 用线程等待的方式决定是否关闭连接
                // 其意义是：先在此阻塞，等待获取到服务端的返回后，被唤醒，从而关闭网络连接
                obj.wait();
            }
            if (response != null) {
                future.channel().closeFuture().sync();
            }
            return response;

        } finally {
            group.shutdownGracefully();
        }
    }

    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) throws Exception {
        this.response = msg;
        synchronized (obj) {
            obj.notifyAll();
        }
    }
}
