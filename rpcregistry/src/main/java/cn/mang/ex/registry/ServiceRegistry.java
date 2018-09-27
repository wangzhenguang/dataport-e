package cn.mang.ex.registry;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ServiceRegistry {
    private static final Logger logger = LoggerFactory
            .getLogger(ServiceRegistry.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private String registryAddress;

    public ServiceRegistry(String registryAddress) {
        this.registryAddress = registryAddress;
    }

    private ZooKeeper connectServer() {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected)
                        latch.countDown();
                }
            });
            latch.await();
        } catch (Exception e) {
            logger.error("{}", e);
        }

        return zooKeeper;
    }

    public void register(String data) {
        if (data != null) {
            ZooKeeper zk = connectServer();
            if (zk != null) {
                createNode(zk, data);
            }

        }
    }

    private void createNode(ZooKeeper zk, String data) {
        try {
            byte[] bytes = data.getBytes();
            if (zk.exists(Constant.ZK_REGISTRY_PATH, null) == null) {
                zk.create(Constant.ZK_REGISTRY_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            String path = zk.create(Constant.ZK_DATA_PATH, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.debug("create zookeeper node {}", path);

            System.out.println("create zookeeper node {}" + path);
        } catch (Exception e) {
            logger.error("{}", e);
        }
    }
}
