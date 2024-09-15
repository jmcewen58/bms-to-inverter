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
package com.airepublic.bmstoinverter.core;

import com.airepublic.bmstoinverter.core.bms.data.BatteryPack;

/**
 * Enables adjusting of the {@link Inverter} settings after initialization and manipulation of send-
 * or receive-frames.
 */
public abstract class InverterPlugin extends AbstractPlugin<Inverter> {

    /**
     * Called after the battery information has been aggregated but before creating send frames to
     * manipulate the aggregated battery information.
     *
     * @param aggregatedPack the aggregated {@link BatteryPack} information
     */
    public void onBatteryAggregation(final BatteryPack aggregatedPack) {
    }
}
