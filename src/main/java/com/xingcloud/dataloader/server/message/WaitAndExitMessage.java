package com.xingcloud.dataloader.server.message;

/**
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class WaitAndExitMessage implements DataLoaderMessage{
    static public final String prefix="waitAndExit";
    public void init(String message) {

    }

    @Override
    public String toString(){
        return prefix;
    }
}
