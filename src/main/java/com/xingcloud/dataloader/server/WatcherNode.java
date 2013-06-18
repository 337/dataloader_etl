package com.xingcloud.dataloader.server;

import com.xingcloud.dataloader.dao.MessageQueue;

/**
 * Author: qiujiawei
 * Date:   12-5-17
 */
public class WatcherNode {
    String ip;
    private MessageQueue messageQueue;
    public WatcherNode(String ip){
        this.ip=ip;
        messageQueue=new MessageQueue(ip);
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
    public MessageQueue getMessageQueue(){
        return messageQueue;
    }
}
