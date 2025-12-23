package org.example.main;

import org.apache.log4j.Logger;

public class LoggerMessageClass {

    private final CustomAppender customAppender;

    // The root logger can collect everything of log messages
    private final Logger rootLogger = Logger.getRootLogger();
    private final Logger logger = Logger.getLogger(LoggerMessageClass.class);

    public LoggerMessageClass(CustomAppender customAppender) {
        this.customAppender = customAppender;
    }

    // TODO. 确定移除添加到特定Logger的Appender
    // Create your own Appender and add it to the logger you want
    // Remove the Appender when it's done, otherwise memory leak !
    public void testLogMessage() {
        try {
            logger.addAppender(this.customAppender);
            logger.info("Test");
        } finally {
            logger.removeAppender(this.customAppender);
        }
    }
}
