package cn.mang.ex.registry;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class ServiceDiscovery {
    private static final Logger logger = LoggerFactory
            .getLogger(ServiceDiscovery.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private volatile List<String> dataList = new ArrayList<String>();

    private String registryAddress;


    public ServiceDiscovery(String registryAddress)
    {
        this.registryAddress = registryAddress;
        ZooKeeper zooKeeper = connectServer();
        if(zooKeeper != null){
            watchNode(zooKeeper);
        }
    }

    private void watchNode(final ZooKeeper zooKeeper) {
        try {
            List<String> children = zooKeeper.getChildren(Constant.ZK_REGISTRY_PATH, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                        watchNode(zooKeeper);
                    }
                }
            });

            List<String> dataList = new ArrayList<String>();
            for (String child : children) {
                byte[] data = zooKeeper.getData(Constant.ZK_REGISTRY_PATH + "/" + child, false, null);
                dataList.add(new String(data));
            }

            logger.debug("address {}",dataList);
            this.dataList = dataList;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 发现新节点
     *
     * @return
     */
    public String discover() {
        String data = null;
        int size = dataList.size();
        // 存在新节点，使用即可
        if (size > 0) {
            if (size == 1) {
                data = dataList.get(0);
                logger.debug("using only data: {}", data);
            } else {
                data = dataList.get(ThreadLocalRandom.current().nextInt(size));
                logger.debug("using random data: {}", data);
            }
        }
        return data;
    }

    private ZooKeeper connectServer() {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT,
                    new Watcher() {
                        public void process(WatchedEvent event) {
                            if (event.getState() == Event.KeeperState.SyncConnected) {
                                latch.countDown();
                            }
                        }
                    });
            latch.await();
        } catch (Exception e) {
            logger.error("", e);
        }
        return zk;
    }
}
