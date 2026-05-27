package com.devalere.eventdriven.schema.event;

/**
 * Schema BREAKING : renommage de "customer" en "customerName" + suppression de "amount".
 * <p>
 * C'est un BREAKING CHANGE :
 * - "customer" renomme en "customerName" = les consumers V1/V2/V3 ne trouvent plus "customer"
 * - "amount" supprime = les consumers V1/V2/V3 ne trouvent plus "amount"
 * <p>
 * EXACTEMENT ce qu'il ne faut PAS faire en production.
 */
public record OrderEventBreaking(
        String schemaVersion,     // "BREAKING"
        Long orderId,
        String customerName,      // RENOMME (etait "customer") = BREAKING !
        // "amount" SUPPRIME = BREAKING !
        String email,
        String priority,
        double totalPrice         // NOUVEAU champ a la place d'amount
) {
}
