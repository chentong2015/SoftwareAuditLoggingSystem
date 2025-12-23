package verbose.inactive;

import org.slf4j.Logger;
import org.slf4j.Marker;

// 自定义未被激活的Logger, 调用该Logger将不会输出任何的信息
public class InactiveLogger implements Logger {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public void trace(String string) {
        // inactive logger does nothing
    }

    @Override
    public void trace(String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void trace(String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void trace(String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void trace(String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return false;
    }

    @Override
    public void trace(Marker marker, String string) {
        // inactive logger does nothing
    }

    @Override
    public void trace(Marker marker, String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void trace(Marker marker, String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void trace(Marker marker, String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void trace(Marker marker, String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void debug(String string) {
        // inactive logger does nothing
    }

    @Override
    public void debug(String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void debug(String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void debug(String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void debug(String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return false;
    }

    @Override
    public void debug(Marker marker, String string) {
        // inactive logger does nothing
    }

    @Override
    public void debug(Marker marker, String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void debug(Marker marker, String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void debug(Marker marker, String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void debug(Marker marker, String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public void info(String string) {
        // inactive logger does nothing
    }

    @Override
    public void info(String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void info(String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void info(String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void info(String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return false;
    }

    @Override
    public void info(Marker marker, String string) {
        // inactive logger does nothing
    }

    @Override
    public void info(Marker marker, String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void info(Marker marker, String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void info(Marker marker, String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void info(Marker marker, String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void warn(String string) {
        // inactive logger does nothing
    }

    @Override
    public void warn(String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void warn(String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void warn(String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void warn(String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return false;
    }

    @Override
    public void warn(Marker marker, String string) {
        // inactive logger does nothing
    }

    @Override
    public void warn(Marker marker, String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void warn(Marker marker, String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void warn(Marker marker, String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void warn(Marker marker, String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public void error(String string) {
        // inactive logger does nothing
    }

    @Override
    public void error(String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void error(String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void error(String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void error(String string, Throwable thrwbl) {
        // inactive logger does nothing
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return false;
    }

    @Override
    public void error(Marker marker, String string) {
        // inactive logger does nothing
    }

    @Override
    public void error(Marker marker, String string, Object o) {
        // inactive logger does nothing
    }

    @Override
    public void error(Marker marker, String string, Object o, Object o1) {
        // inactive logger does nothing
    }

    @Override
    public void error(Marker marker, String string, Object... os) {
        // inactive logger does nothing
    }

    @Override
    public void error(Marker marker, String string, Throwable thrwbl) {
        // inactive logger does nothing
    }
}
