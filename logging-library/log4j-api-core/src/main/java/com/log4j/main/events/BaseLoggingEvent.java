package com.log4j.main.events;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;

import java.util.ArrayList;
import java.util.List;

// TODO. 测试日志信息: 通过LogEvent来获取输出日志并判断
public class BaseLoggingEvent {

    private static Appender appender;
    private static List<LogEvent> captorLoggingEvent = new ArrayList<>();

    public static void main(String[] args) {
        LogEvent logEvent = new Log4jLogEvent();
        captorLoggingEvent.add(logEvent);

        LogEvent event = captorLoggingEvent.get(0);
        System.out.println(event.getLevel() == Level.INFO);
        captorLoggingEvent.clear();
    }

    public static void testLogContext() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LoggerListener.class.getName());
        loggerConfig.addAppender(appender, Level.INFO, null);
        ctx.updateLoggers();
    }
}
