package com.flume.sink.kudu;

import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.Operation;

import java.util.List;

/**
 * Interface for an operations producer that produces Kudu Operations from
 * Flume events.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface KuduOperationsProducer extends Configurable, AutoCloseable {
    /**
     * Initializes the operations producer. Called between configure and
     * getOperations.
     * @param kuduClient the KuduTable used to create Kudu Operation objects
     */
    void initialize(KuduClient kuduClient,String table,String customKeys);

    /**
     * Returns the operations that should be written to Kudu as a result of this event.
     * @param event Event to convert to one or more Operations
     * @return List of Operations that should be written to Kudu
     */
    List<Operation> getOperations(Event event);

    /**
     * Cleans up any state. Called when the sink is stopped.
     */
    void close();
}
