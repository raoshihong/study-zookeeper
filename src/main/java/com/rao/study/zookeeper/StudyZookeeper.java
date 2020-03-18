package com.rao.study.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class StudyZookeeper {

    ZooKeeper zooKeeper = null;

    @Before
    public void connect()throws Exception{

        //通过zookeeper客户端连接zookeeper服务
        //连接地址,客户端连接zookeeper的端口号,默认2181,格式为IP1:PORT1,IP2:PORT2,也可以使用hostname:port
        String connectString = "hadoop120:2181,hadoop121:2181,hadoop122:2181";
        // 会话关闭,临时节点存活时间
        int sessionTimeOut = 10*1000;//单位为毫秒
        zooKeeper = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {//默认观察者
            public void process(WatchedEvent watchedEvent) {
                System.out.println("默认观察者"+watchedEvent.getPath());
            }
        });

    }

    @Test
    public void ls() throws Exception{
        //获取子节点列表
        //获取/下的子节点列表,并指明需要进行监听，没有指明自定义的监听器，则使用默认的Watcher
//        List<String> childrens = zooKeeper.getChildren("/",true);
        Stat stat = new Stat();
        List<String> childrens = zooKeeper.getChildren("/aaa", new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("自定义Watcher");
            }
        },stat);
        System.out.println(childrens);
        System.out.println("================");
        System.out.println(stat);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }


    @Test
    public void test()throws Exception{
        register();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }

    public void register() throws Exception{
        List<String> childrens = zooKeeper.getChildren("/aaa", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try{
                    //递归注册监听器
                    System.out.println("递归监听:"+event.getPath());
                    register();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        System.out.println(childrens);
    }

    @Test
    public void create()throws Exception{
        //创建节点,并通过CreateMode设置创建的是什么类型的节点（临时无序，临时有序，永久无序，永久有序）
        // 通过Ids指明AcL列表
        String path = zooKeeper.create("/aaa","abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(path);
    }

    @Test
    public void getStat() throws Exception{
        Stat stat = zooKeeper.exists("/aaa",false);
        //通过stat可以获取节点的相关信息
        if (stat != null) {
            System.out.println("version="+stat.getVersion()+",czxid="+stat.getCzxid());
        }
    }

    @Test
    public void setData()throws Exception{
        Stat stat = zooKeeper.exists("/aaa",false);
        //需要传递节点的版本号,zookeeper会根据版本号进行判断是否需要更新值
        if (stat != null) {
            zooKeeper.setData("/aaa","asdfsf".getBytes(),stat.getVersion());
        }
    }

    @Test
    public void getData() throws Exception{
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/aaa",false,stat);
        System.out.println(new String(data));
    }
}
