package org.example.main;

import org.apache.log4j.Logger;

// TODO. 提供特定的构造器注入参数，提供测试扩展，检测日志输出
public class LoggerInjectionClass {

    private final String name;
    private final String url;
    private final Logger logger;

    public LoggerInjectionClass(String name) {
        this(name, "demo.com/testing", Logger.getLogger("test"));
    }

    // @VisibleForTesting
    // - 自动注入要测试的url(改变发送请求的target)
    // - 注入自定义的Logger, injectLogger中包含自定义的CustomAppender
    public LoggerInjectionClass(String name, String url, Logger injectLogger) {
        this.name = name;
        this.url = url;
        this.logger = injectLogger;
    }

    public void testLogMessage() {
        this.logger.info("info message");
        this.logger.debug("debug");
    }
}
