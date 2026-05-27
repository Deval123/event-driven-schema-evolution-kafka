package com.devalere.eventdriven.schema.event;

/**
 * Schema V1 : le schema original.
 * 3 champs obligatoires.
 */
public record OrderEventV1(
        String schemaVersion,  // "V1"
        Long orderId,
        String customer,
        double amount
) {
}
