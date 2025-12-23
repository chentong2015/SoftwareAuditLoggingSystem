package com.commons.logging;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class DemoFileHandler {

    public static void main(String[] args) throws IOException {
        // 自定义输出的日志文件
        String logPath = "slf4j-commons-logging/log/app.log";
        FileHandler fileHandler = new FileHandler(logPath, 5000, 3, true);
        fileHandler.setFormatter(new SimpleFormatter());
        fileHandler.setLevel(Level.WARNING);

        // 为全局Logger设置File文件输出
        Logger.getGlobal().addHandler(fileHandler);
        Logger loggerGlobal = Logger.getGlobal();

        // TODO. Console默认输出 + 为Logger设置File文件输出
        Logger loggerFull = Logger.getLogger(DemoCommonsLogging.class.getName());
        loggerFull.addHandler(fileHandler);
        loggerFull.info("info log file");
        loggerFull.warning("warning log file");
        loggerFull.log(Level.SEVERE, "severe log file");
    }
}
