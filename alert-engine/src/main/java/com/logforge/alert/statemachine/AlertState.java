package com.logforge.alert.statemachine;

/**
 * The four states in the alert lifecycle.
 *
 * INACTIVE  → normal operation, no issue detected
 * PENDING   → threshold crossed ONCE, but waiting to confirm
 *             (prevents false alarms from brief spikes)
 * FIRING    → confirmed problem, notifications sent
 * RESOLVED  → was FIRING, now back to normal
 *             (sends "all clear" notification)
 */
public enum AlertState {
    INACTIVE, PENDING, FIRING, RESOLVED
}