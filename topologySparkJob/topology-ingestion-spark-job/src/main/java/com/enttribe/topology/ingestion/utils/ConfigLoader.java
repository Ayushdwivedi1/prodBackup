package com.enttribe.topology.ingestion.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigLoader {

    private static final String CONFIG_FILE = "config.properties";
    private static final Properties PROPS = new Properties();

    static {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (in != null) {
                PROPS.load(in);
            }
        } catch (IOException ignored) {
        }
    }

    private ConfigLoader() {
    }

    public static String get(String key) {
        return get(key, null);
    }

    public static String get(String key, String defaultValue) {
        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String propValue = PROPS.getProperty(key);
        return propValue != null ? propValue : defaultValue;
    }

    public static int getInt(String key, int defaultValue) {
        String value = get(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static long getLong(String key, long defaultValue) {
        String value = get(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(value);
    }
}
