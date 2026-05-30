package com.logforge.alert.statemachine;

public enum AlertEventType {
    FIRING,    // problem confirmed — send alert
    RESOLVED   // problem gone — send all-clear

}