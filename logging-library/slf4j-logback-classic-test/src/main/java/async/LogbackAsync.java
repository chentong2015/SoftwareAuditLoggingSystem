package async;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import system.property.LogbackSystemProperty;

// 异步能够提高大量日志信息的输出效率，但是存在丢包情况
// AsyncAppender will drop events of level TRACE, DEBUG and INFO keeping only events of level WARN and ERROR.
// Event loss can be prevented by setting the 'discardingThreshold' property to 0 (zero)
public class LogbackAsync {

    public static void main(String[] args) {
        Logger logger = (Logger) LoggerFactory.getLogger(LogbackSystemProperty.class);
        logger.info("logging {}", "place holder");
        logger.debug("logging debug");
        logger.warn("logging warn");
    }
}
