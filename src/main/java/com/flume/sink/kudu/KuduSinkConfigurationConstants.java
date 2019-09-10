package com.flume.sink.kudu;


import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduSinkConfigurationConstants {
    /**
     * Comma-separated list of "host:port" Kudu master addresses.
     * The port is optional and defaults to the Kudu Java client's default master
     * port.
     */
    public static final String MASTER_ADDRESSES = "masterAddresses";
    public static final String KUDU_NAMESPACE = "namespace";
    /**
     * The name of the table in Kudu to write to.
     */
    public static final String TABLE_NAME = "tableName";
    /*
     */
/**
 * 是否采用自定义key
 *
 * true
 * false
 *
 */
  public static final String CUSTOM_KEY = "customKey";

/**
 * 是否生成随机key？
 * 1，生成随机key
 * 2，生成随机key+字段
 * 3，字段1+字段2
 *
 * uuid
 * col1
 * col1,col2
 *
 *//*
  public static final String KEY_NAME = "keyName";
*/
    /**
     * The fully qualified class name of the KuduOperationsProducer class that the
     * sink should use.
     */
    public static final String PRODUCER = "producer";

    /**
     * Prefix for configuration parameters that are passed to the
     * KuduOperationsProducer.
     */
    public static final String PRODUCER_PREFIX = PRODUCER + ".";

    /**
     * Maximum number of events that the sink should take from the channel per
     * transaction.
     */
    public static final String BATCH_SIZE = "batchSize";

    /**
     * Timeout period for Kudu operations, in milliseconds.
     */
    public static final String TIMEOUT_MILLIS = "timeoutMillis";

    /**
     * Whether to ignore duplicate primary key errors caused by inserts.
     */
    public static final String IGNORE_DUPLICATE_ROWS = "ignoreDuplicateRows";

    public static final String FLUSH_BUFFER_SIZE = "flushBufferSize";
}
