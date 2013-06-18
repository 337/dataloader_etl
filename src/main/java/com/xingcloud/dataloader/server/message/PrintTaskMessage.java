package com.xingcloud.dataloader.server.message;

/**
 * Author: qiujiawei
 * Date:   12-5-17
 */
public class PrintTaskMessage implements DataLoaderMessage{
    static public final String prefix="printTask";
    public void init(String message) {

    }

    @Override
    public String toString(){
        return prefix;
    }
}
