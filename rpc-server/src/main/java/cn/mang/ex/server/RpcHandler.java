package cn.mang.ex.server;

import cn.mang.ex.RpcRequest;
import cn.mang.ex.RpcResponse;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class RpcHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private static final Logger logger = LoggerFactory
            .getLogger(RpcHandler.class);

    private final Map<String, Object> handlerMap;

    public RpcHandler(Map<String, Object> handlerMap) {
        this.handlerMap = handlerMap;
    }


    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) throws Exception {
        RpcResponse response = new RpcResponse();
        response.setRequestId(request.getRequestId());
        //调用具体业务


        String className = request.getClassName();

        /**
         * 拿到真正的实现类
         */
        Object serviceBean = handlerMap.get(className);

        String methodName = request.getMethodName();
        Object[] parameters = request.getParameters();
        Class<?>[] parameterTypes = request.getParameterTypes();
        try {
            Class<?> aClass = Class.forName(className);
            Method method = aClass.getMethod(methodName, parameterTypes);
            // 具体业务类处理的结果
            Object invoke = method.invoke(serviceBean, parameters);

            response.setResult(invoke);
        } catch (Exception e) {
            response.setError(e);
        }

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("{}", cause);
        ctx.close();
    }
}
