package com.log4j.main;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BaseLog4j {

    // 自定义logger日志的名称
    private static final Logger logger = LogManager.getLogger("myLogger");
    private static final Logger newLogger = LogManager.getLogger(BaseLog4j.class.getName());
    private static final Logger newLogger2 = LogManager.getLogger();

    public static void main(String[] args) {
        logger.debug("Main debug message");
        logger.info("Main info message");
        logger.warn("Main warn message");
        logger.error("Main error message");
        System.out.println("-------------");
    }

    private static void testLoggerBasic() {
        // 判读是否开启了指定Level的日志
        if (logger.isDebugEnabled()) {
            // debug() 输出信息的形式可以格式化
            logger.debug("debug message");
            logger.debug("Integer.MAX_VALUE = %,d", Integer.MAX_VALUE);
            logger.debug("Long.MAX_VALUE = %,d", Long.MAX_VALUE);
        }

        logger.printf(Level.ERROR, "error level");
        logger.error("error message"); // By default, output this

        // 提供的lambda expressions只有在trace enabled的时候被执行
        // logger.trace("Some long-running operation returned {}", () -> expensiveOperation());
    }

    // TODO. 获取指定的日志配置文件，完成日志properties的设置
    public void serLog4jPath() {
        String log4JPropertyFile = "log4j.properties";
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(log4JPropertyFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // PropertyConfigurator.configure(p);
    }
}
