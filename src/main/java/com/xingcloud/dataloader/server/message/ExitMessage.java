package com.xingcloud.dataloader.server.message;

/**
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class ExitMessage implements DataLoaderMessage{
    static public final String prefix="exit";
    public void init(String message) {

    }

    @Override
    public String toString(){
        return prefix;
    }
}
