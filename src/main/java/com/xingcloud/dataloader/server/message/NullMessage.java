package com.xingcloud.dataloader.server.message;

/**
 * Author: qiujiawei
 * Date:   12-5-17
 */
public class NullMessage implements DataLoaderMessage{
    static public final String prefix="no_message";

    public void init(String message) {

    }

    @Override
    public String toString(){
        return prefix;
    }
}
