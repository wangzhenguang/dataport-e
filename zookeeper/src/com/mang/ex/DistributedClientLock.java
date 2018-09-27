package com.mang.ex;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 分布式应用锁
 */
public class DistributedClientLock {

    private static final int session_timeout = 2000;

    private String hosts = "anlyslave01:2181,anlyslave02:2181,anlyslave03:2181";
    private String pNode = "locks";
    private ZooKeeper zk;
    private String subNode = "sub";
    private volatile String path;

    public void connectZookeeper() throws Exception {

        zk = new ZooKeeper(hosts, session_timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {


                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged && watchedEvent.getPath().equals("/" + pNode)) {


                    try {
                        // 必须注册监听
                        List<String> children = zk.getChildren("/" + pNode, true);
                        // 获取子节点名称
                        String thisNode = path.substring(("/" + pNode + "/").length());
                        Collections.sort(children);

                        if (children.indexOf(thisNode) == 0) {
                            doWork();
                            //重新注册一把新的锁
                            path = zk.create("/" + pNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL_SEQUENTIAL);
                            System.out.println("process " + path);

                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        });

        // 注册客户端锁
        path = zk.create("/" + pNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        //注册父节点监听
        List<String> children = zk.getChildren("/" + pNode, true);

        // 判断是否只有一个客户端
        if (children.size() == 1) {
            //只有一个客户端 不需要等待锁
            doWork();
            //重新注册锁
            path = zk.create("/" + pNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

    }


    private void doWork() throws InterruptedException, KeeperException {
        try {
            System.out.println("获取到的锁 : " + path);
        } finally {
            //一定要删除当前节点
            System.out.println("执行完成删除锁: " + path);
            zk.delete(this.path, -1);
        }
    }

    public static void main(String[] args) throws Exception {
        new DistributedClientLock().connectZookeeper();
        Thread.sleep(Long.MAX_VALUE);
    }

}
