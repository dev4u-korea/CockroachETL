package com.mobil.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by user on 2018-11-05.
 */
public class DateUtil {

    public static String getCurrentDtm() {

        Date now = new Date();

        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddkkmmss");


        return fmt.format(now);
    }
}
