package org.example.main;

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LoggerMessageTest {

    @Test
    public void testLoggerMessage() {
        CustomAppender appender = new CustomAppender();
        LoggerMessageClass projectClass = new LoggerMessageClass(appender);
        projectClass.testLogMessage();

        LoggingEvent firstLogEntry = appender.getLog().get(0);
        Assertions.assertEquals(firstLogEntry.getLevel(), Level.INFO);
        Assertions.assertEquals(firstLogEntry.getMessage(), "Test");
        Assertions.assertEquals(firstLogEntry.getLoggerName(), "org.example.main.LoggerMessageClass");
    }
}
