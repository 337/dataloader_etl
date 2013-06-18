package com.xingcloud.dataloader.buildtable.buildmysqltable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: qiujiawei
 * Date:   12-6-5
 */
public class UserProperty {
    public enum PropertyType {UPDATE_STRING,ONLY_STRING,UPDATE_INT,ONLY_INT,INC_INT,}

    static public final String registerField="register_time";
    static public final String lastLoginTimeField ="last_login_time";
    static public final String firstPayTimeField ="first_pay_time";
    static public final String lastPatTimeField ="last_pay_time";

    static public final String gradeField="grade";
    static public final String gameTimeField="game_time";
    static public final String payAmountField ="pay_amount";

    static public final String languageField="language";
    static public final String versionField="version";
    static public final String platformField="platform";
    static public final String identifierField="identifier";

    static public final String refField ="ref";

    static public final String customField1 ="key1";
    static public final String customField2 ="key2";
    static public final String customField3 ="key3";
    static public final String customField4 ="key4";
    static public final String customField5 ="key5";

    static public List<String> defaultUserPropertyList=new ArrayList<String>();
    static public List<String> integerUserPropertyList=new ArrayList<String>();
    static public List<String> stringUserPropertyList=new ArrayList<String>();

    static public Map<String,PropertyType> defaultUserPropertyMap=new HashMap<String,PropertyType>();
    static public void newUserProperty(String name,PropertyType type){
        defaultUserPropertyList.add(name);
        defaultUserPropertyMap.put(name,type);
        if(type==PropertyType.UPDATE_STRING || type==PropertyType.ONLY_STRING){
            stringUserPropertyList.add(name);
        }
        else if(type==PropertyType.UPDATE_INT || type==PropertyType.ONLY_INT || type==PropertyType.INC_INT) {
            integerUserPropertyList.add(name);
        }
    }

    static {{
        newUserProperty(registerField,UserProperty.PropertyType.ONLY_STRING);
        newUserProperty(lastLoginTimeField,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(firstPayTimeField,UserProperty.PropertyType.ONLY_STRING);
        newUserProperty(lastPatTimeField,UserProperty.PropertyType.UPDATE_STRING);

        newUserProperty(gradeField,UserProperty.PropertyType.UPDATE_INT);
        newUserProperty(gameTimeField,UserProperty.PropertyType.INC_INT);
        newUserProperty(payAmountField,UserProperty.PropertyType.INC_INT);


        newUserProperty(languageField,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(versionField,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(platformField,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(identifierField,UserProperty.PropertyType.UPDATE_STRING);

        newUserProperty(refField,UserProperty.PropertyType.ONLY_STRING);

        newUserProperty(customField1,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(customField2,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(customField3,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(customField4,UserProperty.PropertyType.UPDATE_STRING);
        newUserProperty(customField5,UserProperty.PropertyType.UPDATE_STRING);

    }}


    static public PropertyType getUserProperty(String name){
        return defaultUserPropertyMap.get(name);
    }
    static public List<String> getUserPropertyList(){
        return defaultUserPropertyList;
    }

    static public boolean isInteger(String name){
        return integerUserPropertyList.contains(name);
    }
    static public boolean isString(String name){
        return stringUserPropertyList.contains(name);
    }
}
