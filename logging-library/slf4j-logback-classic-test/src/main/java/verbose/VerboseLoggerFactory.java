package verbose;

import verbose.inactive.InactiveLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerboseLoggerFactory {

    private int verbose;

    // TODO. Verbose可以根据系统属性动态设置
    public void setVerbose(int verbose) {
        this.verbose = verbose;
    }

    public Logger getLvl0DefaultLogger(Class<?> clazz) {
        if (verbose == 0 || verbose == 1 || verbose == 99) {
            return LoggerFactory.getLogger(clazz);
        }
        return new InactiveLogger();
    }

    // 该Logger的输出只对verbose=1和99可见
    public Logger getLvl1DefaultLogger(Class<?> clazz) {
        if (verbose == 1 || verbose == 99) {
            return LoggerFactory.getLogger(clazz);
        }
        return new InactiveLogger();
    }

    // 该Logger的输出只对verbose=99可见
    public Logger getLvl99DefaultLogger(Class<?> clazz) {
        if (verbose == 99) {
            return LoggerFactory.getLogger(clazz);
        }
        return new InactiveLogger();
    }
}
