package com.liweidao.common;

import org.apache.kafka.common.protocol.types.Field;

public class ConstantConfig {
    //phoenix 库名
    public static final String HBASE_SCHEMA = "REALTIME6";

    //phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:master,slave1,slave2:2181";

    //clickHouse常量配置
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://192.168.31.145:8123/realtime";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
