package com.xingcloud.dataloader.etloutput;


import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.util.Constants;
import com.xingcloud.util.manager.DateManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * User: IvyTang
 * Date: 12-11-22
 * Time: 下午4:23
 */
public class EventOutput {

    private static final Log LOG = LogFactory.getLog(EventOutput.class);


    private static EventOutput instance;

    private EventOutput() {
        //do nothin
    }


    public static EventOutput getInstance() throws IOException {
        if (instance == null)
            instance = new EventOutput();
        return instance;
    }

    public  void write(String project,List<Event> events) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Event event : events) {
            stringBuilder.append(project);
            stringBuilder.append("\t");
            stringBuilder.append(event.toString());
            stringBuilder.append("\n");
        }
        LOG.info(stringBuilder.toString());
    }


}
