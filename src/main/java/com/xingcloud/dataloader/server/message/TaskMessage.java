package com.xingcloud.dataloader.server.message;

/**
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class TaskMessage implements DataLoaderMessage {
    static public final String prefix = "task";
    private String date;
    private int index;
    private String project;
    private String runType;
    private String taskPriority;

    public TaskMessage() {
    }

    public TaskMessage(String date, int index, String project, String runType, String taskPriority) {
        this.date = date;
        this.index = index;
        this.project = project;
        this.runType = runType;
        this.taskPriority = taskPriority;
    }

    public void init(String message) {
        String[] temp = message.split(",");
        this.date = temp[1];
        this.index = Integer.valueOf(temp[2]);
        this.project = temp[3];
        this.runType = temp[4];
        this.taskPriority = temp[5];
    }

    @Override
    public String toString() {
        return prefix + "," + date + "," + index + "," + project + "," + runType + "," + taskPriority;
    }

    public String getDate() {
        return date;
    }

    public int getIndex() {
        return index;
    }

    public String getProject() {
        return project;
    }

    public String getRunType() {
        return runType;
    }

    public String getTaskPriority() {
        return taskPriority;
    }
}
