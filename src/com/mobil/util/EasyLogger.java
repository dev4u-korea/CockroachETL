package com.mobil.util;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;



/**
 * Created by user on 2018-04-16.
 */
public class EasyLogger {

    /*
    private static Logger logger = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    static {
        logger.setLevel(Level.INFO);

        System.out.println("root logger = " + Logger.ROOT_LOGGER_NAME);
    }
    */

    private static Logger easyLogger = (Logger)LoggerFactory.getLogger("src.com.mobil.util.EasyLogger");

    static {
        easyLogger.setLevel(Level.INFO);
    }

    public static void info(String strLog) {
        easyLogger.info(strLog);
    }

    public static void warn(String strLog) { easyLogger.warn(strLog); }

    public static void error(String strLog) {easyLogger.error(strLog); }

}
