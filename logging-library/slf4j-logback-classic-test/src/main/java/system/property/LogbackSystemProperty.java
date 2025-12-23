package system.property;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

public class LogbackSystemProperty {

    public static void main(String[] args) {
        System.setProperty("logFileDir", "/log"); // 系统的绝对路径
        System.setProperty("logFileName", "logger");
        System.setProperty("LoggerLevel", "ERROR"); // 设置logger的输出级别

        // TODO. 获取到默认的root logger输出: 受LoggerLevel约束
        Logger loggerRoot = (Logger) LoggerFactory.getLogger(LogbackSystemProperty.class);
        loggerRoot.debug("logging debug");
        loggerRoot.info("logging info {}", "place holder");
        loggerRoot.warn("logging warn");
        loggerRoot.error("logging error");

        // TODO. 获取特定的file logger输出: 受LoggerLevel约束
        Logger loggerFile = (Logger) LoggerFactory.getLogger("file_com.example");
        loggerFile.debug("file logging debug");
        loggerFile.info("file logging info {}", "place holder");
        loggerFile.warn("file logging warn");
        loggerFile.error("file logging error");

        // TODO. 获取特定的console logger输出: level="INFO"
        Logger loggerConsole = (Logger) LoggerFactory.getLogger("console_com.example");
        loggerConsole.debug("console logging debug");
        loggerConsole.info("console logging info {}", "place holder");
        loggerConsole.warn("console logging warn");
        loggerConsole.error("console logging error");
    }
}
