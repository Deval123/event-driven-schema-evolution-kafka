package com.devalere.eventdriven.schema.event;

/**
 * Schema V3 : ajout d'un champ optionnel "priority".
 * Compatible backward et forward.
 */
public record OrderEventV3(
        String schemaVersion,  // "V3"
        Long orderId,
        String customer,
        double amount,
        String email,          // depuis V2 - optionnel
        String priority        // NOUVEAU - optionnel ("HIGH", "MEDIUM", "LOW", ou null)
) {
}
