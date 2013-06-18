package com.xingcloud.util.manager;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * Author: qiujiawei
 * Date:   11-7-15
 */
public class NetManager {

    private String localIp;
    private String netIp;
    private String hostName;

    public NetManager(){
            init();
    }

    private void init(){
        storeIp();
        storeHostname();
    }
     /**
     * store the Site Local IP and public IP for this machine
     */
     private void storeIp() {
        try {
            Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
               InetAddress ip = null;
               while (netInterfaces.hasMoreElements()) {
                NetworkInterface ni = netInterfaces.nextElement();
                Enumeration<InetAddress> address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                 ip = address.nextElement();
                    if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress()&& !ip.getHostAddress().contains(":")) {
                        netIp = ip.getHostAddress();
                    }
                    if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")){
                        localIp = ip.getHostAddress();
                    }
                }
               }
              } catch (SocketException e) {
            e.printStackTrace();
        }
     }

    /**
     * store the hostname for this machine
     *
     */
     private void storeHostname(){
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    /**
     * single patten
     */
    private static NetManager instance;
    private static NetManager getInstance(){
        if(instance==null) instance=new NetManager();
        return instance;
    }

    /**
     * get the  Site Local Ip
     * @return the  Site Local Ip
     */
    public static String getSiteLocalIp(){
        return getInstance().localIp;
    }

    /**
     * get the public Ip
     * @return the public Ip
     */
    public static String getPublicIp(){
        return getInstance().netIp;
    }

    /**
     * get the hostname
     * @return the hostname
     */
    public static String getHostName(){
        return getInstance().hostName;
    }
}
