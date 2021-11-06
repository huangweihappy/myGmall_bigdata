package com.hw.zookeeper;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ZooKeeperTest {

    private ZooKeeper zk ;

    /**
     * String path,
     * byte[] data,
     * List<ACL> acl,
     * CreateMode createMode
     */
    @Test
    public void TestCreate() throws KeeperException, InterruptedException {

        
        zk.create("/xiyou","sunsun".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    /**
     * 获取连接对象
     * @throws IOException
     */
    @Before
    public void zooKeeperBefore() throws IOException {
        String connectionStr  = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int  sessionTimeout = 10000;
          zk  =  new ZooKeeper(connectionStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("+++++++++process()方法+++++++++");
            }
        });



    }

    @After
    public void close() throws InterruptedException {
        zk.close();
    }


}
