package com.xingcloud.dataloader.etloutput;

import com.xingcloud.dataloader.lib.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * User: IvyTang
 * Date: 13-1-4
 * Time: 上午11:27
 */
public class DelayOutput {

    private static final Log LOG = LogFactory.getLog(DelayOutput.class);

    private static DelayOutput instance;

    private DelayOutput() {
        //do nothin
    }


    public static DelayOutput getInstance() throws IOException {
        if (instance == null)
            instance = new DelayOutput();
        return instance;
    }

    public void write(String project, List<Event> events) throws IOException {
        if (events.size() == 0)
            return;
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
