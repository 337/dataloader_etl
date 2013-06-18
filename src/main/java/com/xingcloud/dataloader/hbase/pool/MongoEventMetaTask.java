package com.xingcloud.dataloader.hbase.pool;

import com.xingcloud.dataloader.hbase.table.EventMetaTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;

/**
 * Author: qiujiawei
 * Date:   12-3-27
 */
public class MongoEventMetaTask extends BaseTask {
	public static final Log LOG = LogFactory.getLog(MongoEventMetaTask.class);
	static private String type = "monodb_event";
	private EventMetaTable eventMetaTable;
	private String project;
	private long start;

	public MongoEventMetaTask(String project, EventMetaTable eventMetaTable) {
		this.eventMetaTable = eventMetaTable;
		this.project = project;
	}

	public void run() {
		try {
			start = System.currentTimeMillis();
			int flush = eventMetaTable.flushMongodb();
			long end = System.currentTimeMillis();
			Date date = new Date();
			LOG.info("taskNumber:"+getTaskNumber()+"\tproject:"+project+"\tusingTime:"+(end-start)+"\trecord:"+flush);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return " taskNumber=" + getTaskNumber() + "type:" + type + " time:" + (System.currentTimeMillis() - start) + " project:" + project;
	}
}
