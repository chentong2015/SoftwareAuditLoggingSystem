package logback;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.LoggerFactory;

public class CustomLogContext {

    public void createLogger() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        // PatternLayoutEncoder extends PatternLayoutEncoderBase<ILoggingEvent>
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern("%r %thread %level - %msg%n");
        encoder.start();

        // TODO. 注意泛型中特化的Event类型
        FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
        fileAppender.setContext(loggerContext);
        fileAppender.setName("timestamp");
        fileAppender.setFile("log/" + System.currentTimeMillis()+".log");
        fileAppender.setEncoder(encoder);
        fileAppender.start();

        Logger logbackLogger = loggerContext.getLogger("Main");
        logbackLogger.addAppender(fileAppender);
        logbackLogger.debug("hello");

        // OPTIONAL: print logback internal status messages
        StatusPrinter.print(loggerContext);
    }
}
