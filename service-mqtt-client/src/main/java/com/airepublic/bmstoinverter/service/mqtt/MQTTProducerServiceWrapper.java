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

import com.airepublic.bmstoinverter.core.service.IMQTTProducerService;

/**
 * The implementation of the {@link IMQTTProducerService} using the ActiveMQ Artemis implementation.
 */
public class MQTTProducerServiceWrapper implements IMQTTProducerService {
    private IMQTTProducerService impl = null;

    public MQTTProducerServiceWrapper() {

    }

    @Override
    public MQTTProducerServiceWrapper connect(final String locator, final String address, final String username, final String password) throws IOException {
        impl = new MQTTHAProducerService();
        try {
            impl.connect(locator, address, username, password);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return this;
    }


    @Override
    public boolean isRunning() {
        return impl.isRunning();
    }


    @Override
    public void sendMessage(final String content) throws IOException {
        try {
            impl.sendMessage(content);
        } catch (final Exception e) {
            throw new IOException("Could not send MQTT message on topic " + content, e);
        }
    }

    @Override
    public void stop() {
        try {
            impl.stop();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to stop MQTT producer!", e);
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }



}
