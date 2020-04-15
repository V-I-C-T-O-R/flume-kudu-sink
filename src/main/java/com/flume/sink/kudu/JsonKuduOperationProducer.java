package com.flume.sink.kudu;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JsonKuduOperationProducer implements KuduOperationsProducer {

    private static final Logger logger = LoggerFactory.getLogger(JsonKuduOperationProducer.class);
    private static final String INSERT = "insert";
    private static final String UPSERT = "upsert";
    private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);
    public static final String ENCODING_PROP = "encoding";
    public static final String DEFAULT_ENCODING = "utf-8";
    public static final String OPERATION_PROP = "operation";
    public static final String DEFAULT_OPERATION = UPSERT;
    public static final String SKIP_MISSING_COLUMN_PROP = "skipMissingColumn";
    public static final boolean DEFAULT_SKIP_MISSING_COLUMN = false;
    public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
    public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = false;
    public static final String WARN_UNMATCHED_ROWS_PROP = "skipUnmatchedRows";
    public static final boolean DEFAULT_WARN_UNMATCHED_ROWS = true;

    private String tableName;
    private KuduClient client;
    private String customKeys;
    private Charset charset;
    private String operation;
    private boolean skipMissingColumn;
    private boolean skipBadColumnValue;
    private boolean warnUnmatchedRows;
    public void JsonKuduOperationsProducer() {
    }
    @Override
    public void configure(Context context) {
        String charsetName = context.getString(ENCODING_PROP, DEFAULT_ENCODING);
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException e) {
            throw new FlumeException(
                    String.format("Invalid or unsupported charset %s", charsetName), e);
        }
        operation = context.getString(OPERATION_PROP, DEFAULT_OPERATION).toLowerCase();
        Preconditions.checkArgument(
                validOperations.contains(operation),
                "Unrecognized operation '%s'",
                operation);
        skipMissingColumn = context.getBoolean(SKIP_MISSING_COLUMN_PROP,
                DEFAULT_SKIP_MISSING_COLUMN);
        skipBadColumnValue = context.getBoolean(SKIP_BAD_COLUMN_VALUE_PROP,
                DEFAULT_SKIP_BAD_COLUMN_VALUE);
        warnUnmatchedRows = context.getBoolean(WARN_UNMATCHED_ROWS_PROP,
                DEFAULT_WARN_UNMATCHED_ROWS);
    }
    @Override
    public void initialize(KuduClient kuduClient,String table,String customKeys) {
        this.client = kuduClient;
        this.tableName = table;
        this.customKeys = customKeys;
    }
    @Override
    public List<Operation> getOperations(Event event) throws FlumeException {
        String raw = new String(event.getBody(), charset);
        List<Operation> ops = Lists.newArrayList();
        if(raw != null && !raw.isEmpty()) {
            Map<String, String> rawMap = JSON.parseObject(event.getBody(),Map.class);
            List<String> keys = Arrays.asList(this.customKeys.split(","));
            List<ColumnSchema> columns = new ArrayList();
            for(String key : keys){
                columns.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.STRING).key(true).nullable(false).build());
            }
            for (Map.Entry<String, String> entry : rawMap.entrySet()) {
                if (!keys.contains(entry.getKey())){
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(entry.getKey(), Type.STRING).key(false).nullable(true).build());
                }
            }

            try{
                if (!this.client.tableExists(this.tableName)) {
                    Schema schema = new Schema(columns);
                    logger.info("表{}不存在,开始创建",this.tableName);
                    this.client.createTable(this.tableName, schema,new CreateTableOptions().setRangePartitionColumns(keys));
                    logger.info("表{}创建完成",this.tableName);
                }

                KuduTable kubuTable = this.client.openTable(this.tableName);
                Schema schema = kubuTable.getSchema();
                Operation op = null;
                switch (operation) {
                    case UPSERT:
                        op = kubuTable.newUpsert();
                        break;
                    case INSERT:
                        op = kubuTable.newInsert();
                        break;
                    default:
                        throw new FlumeException(
                                String.format("Unrecognized operation type '%s' in getOperations(): " +
                                        "this should never happen!", operation));
                }
                PartialRow row = op.getRow();
                for (ColumnSchema col : schema.getColumns()) {
                    try {
                        String colName = col.getName();

                        String colValue = String.valueOf(rawMap.get(colName));
                        coerceAndSet(colValue, colName, Type.STRING, row);
                    } catch (NumberFormatException e) {
                        String msg = String.format(
                                "Raw value '%s' couldn't be parsed to type %s for column '%s'",
                                raw, col.getType(), col.getName());
                        logOrThrow(skipBadColumnValue, msg, e);
                    } catch (IllegalArgumentException e) {
                        String msg = String.format(
                                "Column '%s' has no matching group in '%s'",
                                col.getName(), raw);
                        logOrThrow(skipMissingColumn, msg, e);
                    } catch (Exception e) {
                        throw new FlumeException("Failed to create Kudu operation", e);
                    }
                }
                ops.add(op);
            }catch (KuduException e){
                e.printStackTrace();
                logger.error("unknow error:",e);
            }
        }

        return ops;
    }
    /**
     * Coerces the string `rawVal` to the type `type` and sets the resulting
     * value for column `colName` in `row`.
     *
     * @param rawVal the raw string column value
     * @param colName the name of the column
     * @param type the Kudu type to convert `rawVal` to
     * @param row the row to set the value in
     * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
     */
    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
            throws NumberFormatException {
        switch (type) {
            case INT8:
                row.addByte(colName, Byte.parseByte(rawVal));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rawVal));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rawVal));
                break;
            case INT64:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            case BINARY:
                row.addBinary(colName, rawVal.getBytes(charset));
                break;
            case STRING:
                if(rawVal == null || "null".equals(rawVal)|| "".equals(rawVal)){
                    row.setNull(colName);
                }else{
                    row.addString(colName, rawVal);
                }
                break;
            case BOOL:
                row.addBoolean(colName, Boolean.parseBoolean(rawVal));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rawVal));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rawVal));
                break;
            case UNIXTIME_MICROS:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
        }
    }
    private void logOrThrow(boolean log, String msg, Exception e)
            throws FlumeException {
        if (log) {
            logger.warn(msg, e);
        } else {
            throw new FlumeException(msg, e);
        }
    }
    @Override
    public void close() {
    }
}
