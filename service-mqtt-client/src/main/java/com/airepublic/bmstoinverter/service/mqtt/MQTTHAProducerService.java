/**
 * This software is free to use and to distribute in its unchanged form for private use.
 * Commercial use is prohibited without an explicit license agreement of the copyright holder.
 * Any changes to this software must be made solely in the project repository at https://github.com/ai-republic/bms-to-inverter.
 * The copyright holder is not liable for any damages in whatever form that may occur by using this software.
 *
 * (c) Copyright 2022 and onwards - Torsten Oltmanns
 *
 * @author Torsten Oltmanns - bms-to-inverter''AT''gmail.com
 */
package com.airepublic.bmstoinverter.service.mqtt;

import java.io.IOException;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.IMqttAsyncClient;
//import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
//import org.eclipse.paho.mqttv5.common.MqttException;
//import org.eclipse.paho.mqttv5.common.MqttMessage;
//import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.airepublic.bmstoinverter.core.service.IMQTTProducerService;

/**
 * The implementation of the {@link IMQTTProducerService} using the Eclipse Paho implementation.
 */
public class MQTTHAProducerService implements IMQTTProducerService {
    private final static Logger LOG = LoggerFactory.getLogger(MQTTHAProducerService.class);
    private boolean running = false;
    private IMqttAsyncClient client = null;
    private String topic;
    private String broker;
    int qos = 2;

    public static IMQTTProducerService provider() {
        return new MQTTHAProducerService();
    }

    @Override
    public MQTTHAProducerService connect(final String broker, final String topic, final String username, final String password) throws IOException {
        this.broker = broker;
        this.topic = topic;
        String clientId = "Bms-to-Inverter";

        try {
            MemoryPersistence persistence = new MemoryPersistence();
            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(false);
            connOpts.setUserName(username);
            connOpts.setPassword(password.getBytes());
            client = new MqttAsyncClient(broker, clientId, persistence);
            LOG.info("Connecting to broker: " + broker);
            client.connect(connOpts).waitForCompletion();
            //token.waitForCompletion();
            LOG.info("Connected MQTT producer at {} to topic {}", broker, topic);
            running = true;
            return this;
        } catch (final Exception e) {
            LOG.error("Error starting MQTT producer service!", e);
            //if (e instanceof MqttException) logErrorInfo((MqttException)e);
            try {
                close();
            } catch (final Exception e1) {
            }

            throw new IOException("Could not create MQTT producer client at " + broker + " on topic " + topic, e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void sendMessage(final String content) throws IOException {
        try {
            LOG.debug("Publishing message: "+content);
            client.publish(topic, content.getBytes(), qos, true).waitForCompletion();
            //token.waitForCompletion();
            LOG.debug("Disconnected");
        } catch (final Exception e) {
            //if (e instanceof MqttException) logErrorInfo((MqttException)e);
            throw new IOException("Could not send MQTT message on topic " + topic, e);
        }
    }


    @Override
    public void stop() {
        try {
            client.close();
            running = false;
        } catch (final Exception e) {
            //if (e instanceof MqttException) logErrorInfo((MqttException)e);
            throw new RuntimeException("Failed to stop MQTT producer!", e);
        }
    }


    @Override
    public void close() throws Exception {
        try {
            stop();
            LOG.info("Shutting down MQTT producer on '{}'...OK", broker);
        } catch (final Exception e) {
            LOG.error("Shutting down MQTT producer on '{}'...FAILED", broker, e);
        }
    }

    /*
    private void logErrorInfo(MqttException me) {
        LOG.debug("reason "+me.getReasonCode());
        LOG.debug("msg "+me.getMessage());
        LOG.debug("loc "+me.getLocalizedMessage());
        LOG.debug("cause "+me.getCause());
        LOG.debug("excep "+me);
    }
        */

    /**
     * Main method to test the producer.
     *
     * @param args none
     */
    /*
     public static void main(final String[] args) {

        String topic        = "bmstoinverter";
        String content      = "Message from MqttPublishSample";
        int qos             = 2;
        String broker       = "tcp://127.0.0.1:1883";
        String clientId     = "Bms-to-Inverter";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(false);
            MqttAsyncClient sampleClient = new MqttAsyncClient(broker, clientId, persistence);
            System.out.println("Connecting to broker: " + broker);
            IMqttToken token = sampleClient.connect(connOpts);
            token.waitForCompletion();
            System.out.println("Connected");
            System.out.println("Publishing message: "+content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            token = sampleClient.publish(topic, message);
            token.waitForCompletion();
            System.out.println("Disconnected");
            System.out.println("Close client.");
            sampleClient.close();
            System.exit(0);
        } catch (Exception e) {
            LOG.error("Error occured", e);

        }
        /*
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    }
    */

}
