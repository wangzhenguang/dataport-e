package com.mang.ex;

import org.apache.zookeeper.*;

public class DistributedServer {

    private String hosts = "anlyslave01:2181,anlyslave02:2181,anlyslave03:2181";

    private static final int sessionTimeout = 2000;
    private static final String parentNode = "/servers";

    private ZooKeeper zk = null;

    public void getConnect() throws Exception {

        zk = new ZooKeeper(hosts, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
                System.out.println(event.getType() + "---" + event.getPath());
                try {
                    zk.getChildren("/", true);
                } catch (Exception e) {
                }
            }
        });

    }

    public void registerServer(String hostname) throws Exception {

        String create = zk.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online.." + create);

    }

    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname + "start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws Exception {
        DistributedServer server = new DistributedServer();
        server.getConnect();
        server.registerServer(args[0]);
        server.handleBussiness(args[0]);
    }
}
