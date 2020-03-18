package com.rao.study.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 编写一个客户端,客户端注册到zookeeper服务中，并获取对应节点下到所有服务器信息
 */
public class DistributeClient {

    private ZooKeeper zooKeeper = null;
    private static final String CONNECTSTRING = "hadoop120:2181,hadoop121:2181,hadoop122:2181";
    private int sessionTimeout = 10*1000;
    private String parentPath = "/distributeServer";

    //获取连接
    public void getConnection()throws Exception{
        zooKeeper = new ZooKeeper(CONNECTSTRING,sessionTimeout,(event)->{});
    }

    /**
     * 获取服务列表
     */
    public void getServerList()throws Exception{
        List<String> childrens = zooKeeper.getChildren(parentPath,(event -> {

        }));

        //遍历子节点,获取所有服务当信息
        childrens.stream().forEach(childPath -> {
            try{
                String path = parentPath + "/" + childPath;
                Stat stat = zooKeeper.exists(path,true);
                if (stat != null) {
                    byte[] data = zooKeeper.getData(path,true,new Stat());
                    System.out.println(new String(data));
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    public void bussiness() throws Exception{
        //处理其他业务
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //设置业务处理超时,则退出服务
        countDownLatch.await(1000, TimeUnit.SECONDS);
    }

    public static void main(String[] args)throws Exception {
        DistributeClient client = new DistributeClient();
        client.getConnection();
        client.getServerList();
        client.bussiness();
    }
}
