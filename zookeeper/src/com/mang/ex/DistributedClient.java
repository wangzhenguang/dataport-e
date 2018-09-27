package com.mang.ex;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedClient {
    private static final int session_timeout = 2000;
    private String hosts = "anlyslave01:2181,anlyslave02:2181,anlyslave03:2181";

    private static final String pNode = "servers";
    private ZooKeeper zooKeeper;
    private volatile List<String> serverList;

    public void getConnect() throws IOException {

        zooKeeper = new ZooKeeper(hosts, session_timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws Exception {
        List<String> children = zooKeeper.getChildren(pNode, true);

        List<String> servers = new ArrayList<>();

        for (String child : children) {
            byte[] data = zooKeeper.getData(pNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        this.serverList = servers;
        System.out.println(serverList);
    }


    public static void main(String[] args) throws Exception {
        DistributedClient distributedClient = new DistributedClient();
        distributedClient.getConnect();
        distributedClient.getServerList();
        distributedClient.handleBussiness();
    }

}
