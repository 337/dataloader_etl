package com.xingcloud.dataloader.buildtable.buildmysqltable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: qiujiawei Date: 12-6-7
 */
public class UserPropertyReducer extends
        Reducer<Text, Text, Text, NullWritable> {
    static String cfName = "user";

    private static enum COUNTER {TOTAL_RECORDS, DISCARD_RECORDS}

    ;

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String value = "";
        int total = 0;
        boolean first = true;
        while (values.iterator().hasNext()) {
            value = values.iterator().next().toString();
            if (first) {
                if (value != null && !"".equals(value)) {
                    context.write(createKeyValue(key.toString(), value),
                            NullWritable.get());
                    context.getCounter(COUNTER.TOTAL_RECORDS).increment(1);
                }
                first = false;
            }
            total++;
        }
    }

    private Text createKeyValue(String uid, String str) {
        Map<String, String> cacheMap = new HashMap<String, String>();

        for (String kv : str.split("\t")) {
            String[] temp = kv.split("=");
            if (temp[0].equals("ref")) {
                String refValue = kv.substring(4);
                if (refValue.equals("unknown") || refValue.equals("sgfrom=") || refValue.equals("xafrom=") ||
                        refValue.equals("sgfrom") || refValue.equals("xafrom"))
                    cacheMap.put(temp[0], null);
                else
                    cacheMap.put(temp[0], refValue);
            } else if (temp[0].equals("register_time")
                    || temp[0].equals("last_login_time")
                    || temp[0].equals("first_pay_time")
                    || temp[0].equals("last_pay_time")) {
                if (temp[1].equals("1970-01-01"))
                    cacheMap.put(temp[0], null);
                else
                    cacheMap.put(temp[0], temp[1]);
            } else if (temp[0].equals("platform") || temp[0].equals("language")
                    || temp[0].equals("version")) {
                cacheMap.put(temp[0], temp[1]);
            } else if (temp[0].equals("grade") || temp[0].equals("pay_amount")
                    || temp[0].equals("game_time")) {
                cacheMap.put(temp[0], temp[1]);
            } else if (temp[0].equals("appid")) {
                String appid = temp[1];
                if (appid
                        .matches("^[a-zA-Z0-9]([a-zA-Z0-9_.\\-]*)@([a-zA-Z0-9_.\\-]+)_([a-zA-Z0-9_.\\-]+$)")) {
                    int at = appid.indexOf("@");
                    int firstUnderscore = appid.indexOf("_", at);
                    int secondUnderscore = appid.indexOf("_",
                            firstUnderscore + 1);
                    // String project=project.substring(0,at);
                    String platform = appid.substring(at + 1, firstUnderscore);
                    cacheMap.put(UserProperty.platformField, platform);

                    String language = null;
                    if (secondUnderscore == -1) {
                        //Special one such as happyfarmer@vz_de...
                        language = appid.substring(firstUnderscore + 1);
                    } else {
                        language = appid.substring(firstUnderscore + 1,
                                secondUnderscore);
                    }
                    cacheMap.put(UserProperty.languageField, language);

                    if (secondUnderscore != -1) {
                        String identifier = appid.substring(secondUnderscore + 1);
                        cacheMap.put(UserProperty.identifierField, identifier);
                    } else {
                        cacheMap.put(UserProperty.identifierField, "unknown");
                    }
                }
            }
        }
        StringBuilder st = new StringBuilder();
        st.append(uid);
        for (String sqlField : UserProperty.getUserPropertyList()) {
            String value = cacheMap.get(sqlField);

            if (value == null || value.equals("null")) {
                value = "null";
                if (UserProperty.isInteger(sqlField))
                    value = "0";
            }
            st.append("\t").append(value);
        }

        return new Text(st.toString());

    }


}