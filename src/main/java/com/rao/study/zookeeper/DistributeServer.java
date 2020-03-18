package com.rao.study.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 编写一个服务端,将服务端注册到zookeeper上
 */
public class DistributeServer {
    private ZooKeeper zooKeeper = null;
    private static final String CONNECTSTRING = "hadoop120:2181,hadoop121:2181,hadoop122:2181";
    private int sessionTimeout = 10*1000;
    private String parentPath = "/distributeServer";

    //获取连接
    public void getConnection()throws Exception{
        zooKeeper = new ZooKeeper(CONNECTSTRING,sessionTimeout,(event)->{});
    }

    //将当前服务注册到zookeeper中
    public void register() throws Exception{
        //指定在根目录注册一个临时节点,节点存放当前服务到信息,当服务端口与zookeeper服务端连接,则节点自动删除
        zooKeeper.create(parentPath+"/server1","192.168.199.110".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void bussiness() throws Exception{
        //处理其他业务
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //设置业务处理超时,则退出服务
        countDownLatch.await(1000, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception{
        DistributeServer distributeServer = new DistributeServer();
        distributeServer.getConnection();
        distributeServer.register();

        distributeServer.bussiness();
    }

}
