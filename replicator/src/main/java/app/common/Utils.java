package app.common;

public class Utils {

    public static final String DELTA_LOG_DIR = "_delta_log";
    public static final String LAST_CHECKPOINT_FILE = "_last_checkpoint";

    public static String getDeltaLogPath(String tablePath) {
        return tablePath + "/" + DELTA_LOG_DIR;
    }

    public static String getLastCheckpointFile(String tablePath) {
        return tablePath + "/" + DELTA_LOG_DIR + "/" + LAST_CHECKPOINT_FILE;
    }
}
