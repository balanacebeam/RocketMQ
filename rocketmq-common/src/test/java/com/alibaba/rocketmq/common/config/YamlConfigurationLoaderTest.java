package com.alibaba.rocketmq.common.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by diwayou on 2015/10/20.
 */
public class YamlConfigurationLoaderTest {

    private YamlConfigurationLoader yamlConfigurationLoader;

    @Before
    public void before() {
        yamlConfigurationLoader = new YamlConfigurationLoader();
    }

    @Test
    public void loadConfigTest() {
        Config config = yamlConfigurationLoader.loadConfig();

        Assert.assertNotNull(config);
    }
}
