package com.devalere.eventdriven.schema.consumer;

import com.devalere.eventdriven.schema.config.KafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumer V1 : ne connait que { orderId, customer, amount }.
 * <p>
 * - Reçoit un event V1 → tout va bien.
 * - Reçoit un event V2 (avec email) → IGNORE le champ inconnu = FORWARD compatible.
 * - Reçoit un event V3 (avec email + priority) → IGNORE les champs inconnus = FORWARD compatible.
 * - Reçoit un event BREAKING (customer renomme, amount supprime) → ECHEC = BREAKING CHANGE.
 */
@Component
@Slf4j
public class V1Consumer {

    private final ObjectMapper mapper = new ObjectMapper();
    @Getter
    private final List<Map<String, Object>> processedEvents = new ArrayList<>();

    @KafkaListener(topics = KafkaConfig.ORDER_TOPIC, groupId = "v1-consumer-group")
    public void onEvent(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            String version = node.has("schemaVersion") ? node.get("schemaVersion").asText() : "unknown";

            log.info("[CONSUMER-V1] Event recu (schema {}) : {}", version, json);

            // V1 consumer ne lit que : orderId, customer, amount
            Long orderId = node.has("orderId") ? node.get("orderId").asLong() : null;
            String customer = node.has("customer") ? node.get("customer").asText() : null;
            Double amount = node.has("amount") ? node.get("amount").asDouble() : null;

            if (orderId == null || customer == null || amount == null) {
                log.error("[CONSUMER-V1] BREAKING CHANGE ! Champs manquants dans l'event {}.", version);
                log.error("[CONSUMER-V1]   orderId={}, customer={}, amount={}", orderId, customer, amount);
                log.error("[CONSUMER-V1]   Le producer a supprime ou renomme un champ obligatoire !");

                Map<String, Object> result = new LinkedHashMap<>();
                result.put("status", "ECHEC");
                result.put("schema", version);
                result.put("reason", "Champs obligatoires manquants (customer ou amount)");
                result.put("json", json);
                processedEvents.add(result);
                return;
            }

            // Traitement OK (on ignore les champs inconnus comme email, priority...)
            log.info("[CONSUMER-V1] Traite : orderId={}, customer={}, amount={}", orderId, customer, amount);

            // Lister les champs inconnus (pour la demo)
            List<String> unknownFields = new ArrayList<>();
            node.fieldNames().forEachRemaining(field -> {
                if (!List.of("schemaVersion", "orderId", "customer", "amount").contains(field)) {
                    unknownFields.add(field);
                }
            });

            if (!unknownFields.isEmpty()) {
                log.info("[CONSUMER-V1] Champs inconnus ignores (FORWARD compatible) : {}", unknownFields);
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "OK");
            result.put("schema", version);
            result.put("orderId", orderId);
            result.put("customer", customer);
            result.put("amount", amount);
            result.put("unknownFieldsIgnored", unknownFields);
            processedEvents.add(result);

        } catch (Exception e) {
            log.error("[CONSUMER-V1] Erreur de parsing : {}", e.getMessage());
        }
    }

}
