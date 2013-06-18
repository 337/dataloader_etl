package com.xingcloud.dataloader.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * zookeeper清理工具，解决hbase重启的zookeeper状态不对称问题
 * Author: qiujiawei
 * Date:   12-6-28
 */
public class ZookeeperCleaner implements Watcher{



    static public void main(String[] args){
        try{

            Configuration conf = HBaseConfiguration.create();
            String hosts=conf.get("hbase.zookeeper.quorum");
            Watcher watcher=new ZookeeperCleaner();
            ZooKeeper zooKeeper = new ZooKeeper(hosts, 30000, watcher);

            if(args[0].equals("print")) printall(zooKeeper, "/", watcher);
            else if(args[0].equals("del")) delall(zooKeeper,"/",watcher);
            else if(args[0].equals("acl")) acl(zooKeeper,"/",watcher);


            zooKeeper.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {

        }
    }

    private static void acl(ZooKeeper zooKeeper, String path, Watcher watcher) throws InterruptedException, KeeperException {
        List<ACL> result = zooKeeper.getACL(path, null);
        System.out.println(result);
    }

    private static void delall(ZooKeeper zooKeeper, String path, Watcher watcher) throws InterruptedException, KeeperException {
        zooKeeper.delete(path,0);
    }

    public void process(WatchedEvent watchedEvent) {
        //TODO method implementation
    }

    static void printall(ZooKeeper zooKeeper, String path,Watcher watcher) throws InterruptedException, KeeperException {
        List<String> childs = zooKeeper.getChildren(path, watcher);
        System.out.println(path+"| data:"+new String(zooKeeper.getData(path,watcher,null))+" | "+childs);
        for(String child:childs){
            if(path.equals("/")) printall(zooKeeper,path+child,watcher);
            else printall(zooKeeper,path+"/"+child,watcher);
        }
    }
}
