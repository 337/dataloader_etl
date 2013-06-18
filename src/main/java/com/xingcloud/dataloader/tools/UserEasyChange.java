package com.xingcloud.dataloader.tools;

import com.xingcloud.util.Base64Util;
import com.xingcloud.util.Common;

import java.io.*;

/**
 * Author: qiujiawei
 * Date:   12-7-30
 */
public class UserEasyChange {
    public static void main(String[] args){
        try{
            String fileName = args[0];
            String newFileName = fileName + "_change";
            File file = new File(args[0]);
            BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            OutputStreamWriter osw = new OutputStreamWriter( new FileOutputStream(new File(newFileName)));
            String line = null;
            while((line = bf.readLine())!=null){
                osw.write(Base64Util.encodeBase64Uid(line)+"\n");
            }
            osw.close();
            bf.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
