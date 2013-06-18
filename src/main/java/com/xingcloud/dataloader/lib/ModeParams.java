package com.xingcloud.dataloader.lib;

/**
 * Author: qiujiawei
 * Date:   11-11-14
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * the class use for reslove all the input params
 * format:
 *
 * xxx.jar a b -c d -e f g
 * then getOption would return the pair (c,d) or (e,f) by key
 * and the getParams would return the param without the option ahead [a,b,g]
 * means the params must need for the job
 */
public class ModeParams implements Serializable{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private Map<String,String> option=new HashMap<String,String>();
    private List<String> params=new ArrayList<String>();
    private String mode=null;
    public ModeParams(String[] args){
        init(args);
    }

    public void init(String[] args){
        for(int i=0;i<args.length;i++){
            if(args[i]!=null && args[i].length()>0){
                if(args[i].startsWith("-") && i+1<args.length){
                    option.put(args[i].substring(1),args[i+1]);
                    i++;
                }
                else{
                    if(mode==null) mode=args[i];
                    else params.add(args[i]);
                }
            }
        }

    }


    /**
     * return the params array
     * @param index the index of the params
     * @return the params
     */
    public String getParams(int index){
        return params.get(index);
    }

    public int getParamsNumber(){
        return params.size();
    }
    public String getMode(){
        return mode;
    }



    public String getOption(String name,String defaultValue){
        //add some check params function here

        String value=option.get(name);
        if(value!=null){
//            if(name.equals("d")){
//                if(!DateManager.checkDate(value)) {
//                    throw new IllegalArgumentException("date params is illegal");
//                }
//            }
        }
        if(value==null) return defaultValue;
        else return value;
    }
    
    public String getOptionString(){
        return option.toString();
    }

}
