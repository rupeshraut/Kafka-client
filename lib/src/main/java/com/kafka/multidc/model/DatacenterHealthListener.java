package com.kafka.multidc.model;

@FunctionalInterface
public interface DatacenterHealthListener {
    void onHealthChange(DatacenterInfo datacenter, boolean healthy);
}
