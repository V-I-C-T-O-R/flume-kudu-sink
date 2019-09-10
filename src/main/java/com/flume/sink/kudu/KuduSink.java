package com.flume.sink.kudu;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flume.sink.kudu.KuduSinkConfigurationConstants.*;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KuduSink.class);
    private static final Long DEFAULT_BATCH_SIZE = 100L;
    private static final Long DEFAULT_TIMEOUT_MILLIS =
            AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;
    private static final String DEFAULT_KUDU_OPERATION_PRODUCER =
            "com.flume.sink.kudu.JsonKuduOperationProducer";
    private static final boolean DEFAULT_IGNORE_DUPLICATE_ROWS = true;
    private static final Integer DEFAULT_FLUSH_BUFFER_SIZE = 20000;

    private String masterAddresses;
    private String tableName;
    private String customKeys;
    private String namespace;
    private long batchSize;
    private long timeoutMillis;
    private boolean ignoreDuplicateRows;
    private int flushBufferSize;
    private KuduTable table;
    private KuduSession session;
    private KuduClient client;
    private KuduOperationsProducer operationsProducer;
    private SinkCounter sinkCounter;

    public KuduSink() {
        this(null);
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    public KuduSink(KuduClient kuduClient) {
        this.client = kuduClient;
    }

    @Override
    public void start() {
        Preconditions.checkState(table == null && session == null,
                "Please call stop before calling start on an old instance.");

        // client is not null only inside tests
        if (client == null) {
            client = new KuduClient.KuduClientBuilder(masterAddresses).build();
        }
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setTimeoutMillis(timeoutMillis);
        session.setIgnoreAllDuplicateRows(ignoreDuplicateRows);
        session.setMutationBufferSpace(flushBufferSize);

        String realTableName = null;
        if("".equals(namespace)){
            realTableName = tableName;
        }else{
            realTableName = namespace+"."+tableName;
        }
        operationsProducer.initialize(client,realTableName,customKeys);

        super.start();
        sinkCounter.incrementConnectionCreatedCount();
        sinkCounter.start();
    }

    @Override
    public void stop() {
        Exception ex = null;
        try {
            operationsProducer.close();
        } catch (Exception e) {
            ex = e;
            logger.error("Error closing operations producer", e);
        }
        try {
            if (client != null) {
                client.shutdown();
            }
            client = null;
            table = null;
            session = null;
        } catch (Exception e) {
            ex = e;
            logger.error("Error closing client", e);
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        if (ex != null) {
            throw new FlumeException("Error stopping sink", ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Context context) {
        masterAddresses = context.getString(MASTER_ADDRESSES);
        Preconditions.checkNotNull(masterAddresses,
                "Missing master addresses. Please specify property '$s'.",
                MASTER_ADDRESSES);

        tableName = context.getString(TABLE_NAME);
        Preconditions.checkNotNull(tableName,
                "Missing table name. Please specify property '%s'",
                TABLE_NAME);

        namespace = context.getString(KUDU_NAMESPACE,"");
        customKeys = context.getString(CUSTOM_KEY,"");
        batchSize = context.getLong(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        timeoutMillis = context.getLong(TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS);
        ignoreDuplicateRows = context.getBoolean(IGNORE_DUPLICATE_ROWS, DEFAULT_IGNORE_DUPLICATE_ROWS);
        flushBufferSize = context.getInteger(FLUSH_BUFFER_SIZE,DEFAULT_FLUSH_BUFFER_SIZE);
        String operationProducerType = context.getString(PRODUCER);

        // Check for operations producer, if null set default operations producer type.
        if (operationProducerType == null || operationProducerType.isEmpty()) {
            operationProducerType = DEFAULT_KUDU_OPERATION_PRODUCER;
            logger.warn("No Kudu operations producer provided, using default");
        }

        Context producerContext = new Context();
        producerContext.putAll(context.getSubProperties(
                KuduSinkConfigurationConstants.PRODUCER_PREFIX));

        try {
            Class<? extends KuduOperationsProducer> clazz =
                    (Class<? extends KuduOperationsProducer>)
                            Class.forName(operationProducerType);
            operationsProducer = clazz.newInstance();
            operationsProducer.configure(producerContext);
        } catch (Exception e) {
            logger.error("Could not instantiate Kudu operations producer" , e);
            Throwables.propagate(e);
        }
        sinkCounter = new SinkCounter(this.getName());
    }

    public KuduClient getClient() {
        return client;
    }

    @Override
    public Status process() throws EventDeliveryException {
        if (session.hasPendingOperations()) {
            // If for whatever reason we have pending operations, refuse to process
            // more and tell the caller to try again a bit later. We don't want to
            // pile on the KuduSession.
            return Status.BACKOFF;
        }

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();

        txn.begin();

        try {
            long txnEventCount = 0;
            for (; txnEventCount < batchSize; txnEventCount++) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }

                List<Operation> operations = operationsProducer.getOperations(event);
                for (Operation o : operations) {
                    session.apply(o);
                }
//                logger.info(String.format("kudu成功插入%s库%s表",namespace,tableName));
            }

            logger.info("Flushing {} events", txnEventCount);
            List<OperationResponse> responses = session.flush();
            if (responses != null) {
                for (OperationResponse response : responses) {
                    // Throw an EventDeliveryException if at least one of the responses was
                    // a row error. Row errors can occur for example when an event is inserted
                    // into Kudu successfully but the Flume transaction is rolled back for some reason,
                    // and a subsequent replay of the same Flume transaction leads to a
                    // duplicate key error since the row already exists in Kudu.
                    // Note: Duplicate keys will not be reported as errors if ignoreDuplicateRows
                    // is enabled in the config.
                    if (response.hasRowError()) {
                        throw new EventDeliveryException("Failed to flush one or more changes. " +
                                "Transaction rolled back: " + response.getRowError().toString());
                    }
                }
            }

            if (txnEventCount == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else if (txnEventCount == batchSize) {
                sinkCounter.incrementBatchCompleteCount();
            } else {
                sinkCounter.incrementBatchUnderflowCount();
            }

            txn.commit();

            if (txnEventCount == 0) {
                return Status.BACKOFF;
            }

            sinkCounter.addToEventDrainSuccessCount(txnEventCount);
            return Status.READY;

        } catch (Throwable e) {
            txn.rollback();

            String msg = "Failed to commit transaction. Transaction rolled back.";
            logger.error(msg, e);
            if (e instanceof Error || e instanceof RuntimeException) {
                Throwables.propagate(e);
            } else {
                logger.error(msg, e);
                throw new EventDeliveryException(msg, e);
            }
        } finally {
            txn.close();
        }

        return Status.BACKOFF;
    }
}
