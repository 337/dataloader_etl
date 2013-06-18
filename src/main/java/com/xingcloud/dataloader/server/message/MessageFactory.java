package com.xingcloud.dataloader.server.message;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class MessageFactory {
    static public Log LOG= LogFactory.getLog(MessageFactory.class);
    static Map<String,Class> allClass=new TreeMap<String,Class>(){{
        put(ExitMessage.prefix, ExitMessage.class);
        put(WaitAndExitMessage.prefix, WaitAndExitMessage.class);
        put(TaskMessage.prefix, TaskMessage.class);
        put(PrintTaskMessage.prefix,PrintTaskMessage.class);
        put(NullMessage.prefix,NullMessage.class);
    }};
    static public DataLoaderMessage getMessage(String message){
        if(message==null) return new NullMessage();

        try{
            String[] t=message.split(",");
            Class c=getClass(t[0]);
            Object temp=c.newInstance();
            if(temp instanceof  DataLoaderMessage){
                DataLoaderMessage re=(DataLoaderMessage) temp;
                re.init(message);
                return re;
            }
        }
        catch (Exception e){
            LOG.error("message:"+message,e);
        }
        return new NullMessage();
    }
    static public String getString(DataLoaderMessage message){
        return null;
    }
    static Class<DataLoaderMessage> getClass(String message){
        String[] temp=message.split(",");
        return allClass.get(temp[0]);
    }
    static public boolean getFirst(DataLoaderMessage message){
        if(message instanceof TaskMessage) return false;
        else return true;
    }
}
