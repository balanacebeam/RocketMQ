package com.alibaba.rocketmq.common.config;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class YamlConfigurationLoader implements ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

    private final static String DEFAULT_CONFIGURATION = "rocketmq.yaml";

    /**
     * Inspect the classpath to find storage configuration file
     */
    static URL getStorageConfigURL() {
        String configUrl = System.getProperty("rocketmq.config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;

        URL url;
        try {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        } catch (Exception e) {
            ClassLoader loader = Config.class.getClassLoader();
            url = loader.getResource(configUrl);
            if (url == null) {
                String required = "file:" + File.separator + File.separator;
                if (!configUrl.startsWith(required))
                    throw new RuntimeException(String.format(
                            "Expecting URI in variable: [rocketmq.config]. Found[%s]. Please prefix the file with [%s%s] for local " +
                                    "files and [%s<server>%s] for remote files. If you are executing this from an external tool, it needs " +
                                    "to set Config.setClientMode(true) to avoid loading configuration.",
                            configUrl, required, File.separator, required, File.separator));
                throw new RuntimeException("Cannot locate " + configUrl + ".  If this is a local file, please confirm you've provided " + required + File.separator + " as a URI prefix.");
            }
        }

        return url;
    }

    @Override
    public Config loadConfig() {
        return loadConfig(getStorageConfigURL());
    }

    public Config loadConfig(URL url) {
        try {
            logger.info("Loading settings from {}", url);
            byte[] configBytes;
            try (InputStream is = url.openStream()) {
                configBytes = ByteStreams.toByteArray(is);
            } catch (IOException e) {
                // getStorageConfigURL should have ruled this out
                throw new AssertionError(e);
            }

            logConfig(configBytes);

            Constructor constructor = new CustomConstructor(Config.class);
            MissingPropertiesChecker propertiesChecker = new MissingPropertiesChecker();
            constructor.setPropertyUtils(propertiesChecker);
            Yaml yaml = new Yaml(constructor);
            Config result = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
            propertiesChecker.check();
            return result;
        } catch (YAMLException e) {
            throw new RuntimeException("Invalid yaml: " + url, e);
        }
    }

    private void logConfig(byte[] configBytes) {
        Map<Object, Object> configMap = new TreeMap<>((Map<?, ?>) new Yaml().load(new ByteArrayInputStream(configBytes)));
        // these keys contain passwords, don't log them
        for (String sensitiveKey : new String[]{"client_encryption_options", "server_encryption_options"}) {
            if (configMap.containsKey(sensitiveKey)) {
                configMap.put(sensitiveKey, "<REDACTED>");
            }
        }
        logger.info("Node configuration:[{}]", Joiner.on("; ").join(configMap.entrySet()));
    }

    static class CustomConstructor extends Constructor {
        CustomConstructor(Class<?> theRoot) {
            super(theRoot);
        }

        @Override
        protected List<Object> createDefaultList(int initSize) {
            return Lists.newCopyOnWriteArrayList();
        }

        @Override
        protected Map<Object, Object> createDefaultMap() {
            return Maps.newConcurrentMap();
        }

        @Override
        protected Set<Object> createDefaultSet(int initSize) {
            return Sets.newConcurrentHashSet();
        }

        @Override
        protected Set<Object> createDefaultSet() {
            return Sets.newConcurrentHashSet();
        }
    }

    private static class MissingPropertiesChecker extends PropertyUtils {
        private final Set<String> missingProperties = new HashSet<>();

        public MissingPropertiesChecker() {
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<? extends Object> type, String name) throws IntrospectionException {
            Property result = super.getProperty(type, name);
            if (result instanceof MissingProperty) {
                missingProperties.add(result.getName());
            }
            return result;
        }

        public void check() {
            if (!missingProperties.isEmpty()) {
                throw new RuntimeException("Invalid yaml. Please remove properties " + missingProperties + " from your rocketmq.yaml");
            }
        }
    }
}
