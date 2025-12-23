package com.commons.logging;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO. Commons Logging + java.util.logging (JUL) 作为日志实现
public class DemoCommonsLogging {

    public static void main(String[] args) throws IOException {
        // TODO. property文件配置仅Console默认输出
        final String ASSEMBLY_AUDIT_LOGGER_NAME = "assembly-audit";
        Logger loggerConsole = Logger.getLogger(ASSEMBLY_AUDIT_LOGGER_NAME);
        loggerConsole.info("message test chen");
        loggerConsole.warning("this is warn messages");
        loggerConsole.log(Level.SEVERE, "this is severe messages");
    }
}
