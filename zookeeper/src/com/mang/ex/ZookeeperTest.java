package com.mang.ex;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


/**
 * zookeeper api
 */
public class ZookeeperTest {

    private final static String connectString = "anlyslave01:2181,anlyslave02:2181,anlyslave03:2181";
    private final static int connectTimeOut = 3000;
    private ZooKeeper zooKeeper;
    @Before
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectString, connectTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "  --" + watchedEvent.getPath());
                try {
                    zooKeeper.getChildren("/mang",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {

//        String s = zooKeeper.create("/mang/sub", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//         Thread.sleep(2000);
//        List<String> children = zooKeeper.getChildren("/mang", true);
//        System.out.println(s);
//
//        Thread.sleep(Integer.MAX_VALUE);

        String s = zooKeeper.create("/locks" + "/sub", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(s);


    }


    @Test
    public void getRootChildren() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void testNodeExist() throws KeeperException, InterruptedException {
        Stat exists = zooKeeper.exists("/mang", false);
        System.out.println(exists == null ? "not exist " : "exist");
    }


    @Test
    public void getNodeData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/mang", false, null);
        System.out.println(new String(data));
    }

    @Test
    public void delNode() throws KeeperException, InterruptedException {
        zooKeeper.delete("/mang", -1); // -1表示删除所有版本
    }

    @Test
    public void setNodeData() throws KeeperException, InterruptedException {
        zooKeeper.setData("/mang", "data".getBytes(), -1);
    }

}
