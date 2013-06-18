package com.xingcloud.dataloader.etloutput;

import com.xingcloud.dataloader.lib.User;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * User: IvyTang
 * Date: 13-2-1
 * Time: 下午3:14
 */
public class DelayUserOutput {

    private static final Log LOG = LogFactory.getLog(DelayUserOutput.class);

    private static DelayUserOutput instance;

    private DelayUserOutput() {
        //do nothing
    }

    public static DelayUserOutput getInstance() throws IOException {
        if (instance == null)
            instance = new DelayUserOutput();
        return instance;
    }


    public void write(String project, List<User> users) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        for (User user : users) {
            if (user.propertySize() == 0)
                continue;
            stringBuilder.append(project);
            stringBuilder.append("\t");
            stringBuilder.append(user.toString());
            stringBuilder.append("\n");
        }
        LOG.info(stringBuilder.toString());
    }
}
