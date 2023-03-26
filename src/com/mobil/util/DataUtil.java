package com.mobil.util;

/**
 * Created by user on 2018-05-14.
 */
public class DataUtil {


    public static String getNullData(String strOrg, String strDefault) {

        if (strOrg == null)
            return strDefault;
        else
            return strOrg;
    }

    public static String getNullData(String strOrg) {

        if (strOrg == null)
            return "";
        else
            return strOrg;
    }


}
