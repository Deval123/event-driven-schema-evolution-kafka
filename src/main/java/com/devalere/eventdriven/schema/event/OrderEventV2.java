package com.devalere.eventdriven.schema.event;

/**
 * Schema V2 : ajout d'un champ optionnel "email".
 * Compatible backward (V2 consumer lit V1 : email = null).
 * Compatible forward (V1 consumer lit V2 : ignore email).
 */
public record OrderEventV2(
        String schemaVersion,  // "V2"
        Long orderId,
        String customer,
        double amount,
        String email           // NOUVEAU - optionnel (peut etre null)
) {
}
