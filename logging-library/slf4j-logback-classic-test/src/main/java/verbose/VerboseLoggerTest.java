package verbose;

import org.slf4j.Logger;

public class VerboseLoggerTest {

    public static void main(String[] args) {
        System.setProperty("logFileDir", "/log"); // 系统的绝对路径
        System.setProperty("logFileName", "logger");
        System.setProperty("LoggerLevel", "ERROR"); // 设置logger的输出级别

        VerboseLoggerFactory loggerFactory = new VerboseLoggerFactory();
        loggerFactory.setVerbose(99); // 根据verbose参数值确定能够输出的logger

        Logger logger0 = loggerFactory.getLvl0DefaultLogger(VerboseLoggerTest.class);
        logger0.debug("logger0 logging debug");
        logger0.info("logger0 logging info {}", "place holder");
        logger0.warn("logger0 logging warn");
        logger0.error("logger0 logging error");

        Logger logger1 = loggerFactory.getLvl1DefaultLogger(VerboseLoggerTest.class);
        logger1.debug("logger1 logging debug");
        logger1.info("logger1 logging info {}", "place holder");
        logger1.warn("logger1 logging warn");
        logger1.error("logger1 logging error");

        Logger logger99 = loggerFactory.getLvl99DefaultLogger(VerboseLoggerTest.class);
        logger99.debug("logger99 logging debug");
        logger99.info("logger99 logging info {}", "place holder");
        logger99.warn("logger99 logging warn");
        logger99.error("logger99 logging error");
    }
}
