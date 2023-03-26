package com.mobil.util;

/**
 * Created by user on 2019-08-06.
 */
public class CommonUtil {

    public static void sleep(int sec) {
        try {
            Thread.sleep(1000 * sec);
        } catch (Exception ig) {
            ig.printStackTrace();
        }
    }
}
