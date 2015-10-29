package com.alibaba.rocketmq.store.jdbc;

import com.alibaba.rocketmq.store.transaction.jdbc.sharding.route.RouteUtil;
import org.junit.Test;

/**
 * Created by diwayou on 2015/10/29.
 */
public class RouteUtilTest {

    @Test
    public void buildDbNameByProducerGroupTest() {
        String producerGroup = "DIWAYOU_TEST_PRODUCER_GROUP";

        System.out.println(RouteUtil.buildDbNameByProducerGroup(producerGroup, 4));
    }

    @Test
    public void buildDbNameByIndexTest() {
        System.out.println(RouteUtil.buildDbNameByIndex(0));
    }
}
