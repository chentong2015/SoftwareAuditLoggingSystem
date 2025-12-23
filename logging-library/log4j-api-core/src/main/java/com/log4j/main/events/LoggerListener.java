package com.log4j.main.events;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoggerListener implements TracedRequestListener {

    private static final LoggerListener INSTANCE = new LoggerListener();
    private static final Logger LOGGER = LogManager.getLogger(LoggerListener.class);

    // 判断Logger指定的Level配置有没有Enable
    public static boolean isActivated() {
        return LOGGER.isInfoEnabled();
    }

    public static LoggerListener instance() {
        return INSTANCE;
    }

    @Override
    public void onRequestFinished(String propertiesNode) {
        if (isActivated()) {
            LOGGER.info(propertiesNode);
        }
    }
}
