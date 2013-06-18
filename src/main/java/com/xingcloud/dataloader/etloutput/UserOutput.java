package com.xingcloud.dataloader.etloutput;

import com.xingcloud.dataloader.lib.User;
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
 * Time: 下午4:24
 */
public class UserOutput {

    private static final Log LOG = LogFactory.getLog(UserOutput.class);

    private static UserOutput instance;

    private UserOutput() {
        //do nothing
    }

    public static UserOutput getInstance() throws IOException {
        if (instance == null)
            instance = new UserOutput();
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

    public void writeLogEnd(String endlog) {
        LOG.info(endlog + "\n");
    }
}
