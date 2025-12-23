package logback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 根据不同的用户配置，生成不同的Logger对象
public class CustomLoggerFactory {

    private static final String FILE_LOGGER_PREFIX = "file_";
    private static final String CONSOLE_LOGGER_PREFIX = "console_";

    // Check if user configure the log status to console
    public static Logger getConsoleLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(CONSOLE_LOGGER_PREFIX + clazz.getName());
    }

    public static Logger getFileLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(FILE_LOGGER_PREFIX + clazz.getName());
    }
}
