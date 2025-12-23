package com.log4j.main.context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.File;
import java.net.URI;

public class BaseLoggerContext {

    private static final Logger logger = LogManager.getLogger("BaseLoggerContext");

    // 自定义设置log4j日志配置文件位置
    public static void main(String[] args) {
        URI logUri = new File("log4j2.properties").toURI();
        LoggerContext loggerContext = LoggerContext.getContext(false);
        loggerContext.setConfigLocation(logUri);
        loggerContext.reconfigure();

        logger.info("info logger context");
    }
}
