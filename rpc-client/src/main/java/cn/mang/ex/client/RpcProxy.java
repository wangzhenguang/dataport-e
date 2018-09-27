package cn.mang.ex.client;

import cn.mang.ex.RpcRequest;
import cn.mang.ex.RpcResponse;
import cn.mang.ex.registry.ServiceDiscovery;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;

public class RpcProxy {

    private String serverAddress;
    private ServiceDiscovery serviceDiscovery;

    public RpcProxy(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public RpcProxy(ServiceDiscovery serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
    }


    public <T> T create(Class<T> interfacleClass) {
        return (T) Proxy.newProxyInstance(interfacleClass.getClassLoader(), new Class[]{interfacleClass}, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

                RpcRequest request = new RpcRequest();
                request.setClassName(method.getDeclaringClass().getName());
                request.setRequestId(UUID.randomUUID().toString());
                request.setMethodName(method.getName());
                request.setParameterTypes(method.getParameterTypes());
                request.setParameters(args);

                if (serviceDiscovery != null) {
                    serverAddress = serviceDiscovery.discover();
                }

                String[] addressArr = serverAddress.split(":");
                String host = addressArr[0];
                String port = addressArr[1];
                //调用netty  rpc客服端 链接到服务器

                RpcClient rpcClient = new RpcClient(host, Integer.valueOf(port));
                RpcResponse response = rpcClient.send(request);

                if (response.getError() != null) {
                    return response.getError();
                }

                return response.getResult();
            }
        });
    }

}
