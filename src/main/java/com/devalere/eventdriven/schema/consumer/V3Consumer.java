package com.devalere.eventdriven.schema.consumer;

import com.devalere.eventdriven.schema.config.KafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumer V3 : connait { orderId, customer, amount, email, priority }.
 *
 * - Recoit un event V1 -> email=null, priority=null = BACKWARD compatible.
 * - Recoit un event V2 -> priority=null = BACKWARD compatible.
 * - Recoit un event V3 -> tout est la = parfait.
 * - Recoit un event BREAKING -> customer manquant = ECHEC.
 */
@Component
@Slf4j
public class V3Consumer {

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Map<String, Object>> processedEvents = new ArrayList<>();

    @KafkaListener(topics = KafkaConfig.ORDER_TOPIC, groupId = "v3-consumer-group")
    public void onEvent(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            String version = node.has("schemaVersion") ? node.get("schemaVersion").asText() : "unknown";

            log.info("[CONSUMER-V3] Event recu (schema {}) : {}", version, json);

            // V3 consumer lit : orderId, customer, amount, email, priority
            Long orderId = node.has("orderId") ? node.get("orderId").asLong() : null;
            String customer = node.has("customer") ? node.get("customer").asText() : null;
            Double amount = node.has("amount") ? node.get("amount").asDouble() : null;
            String email = node.has("email") ? node.get("email").asText(null) : null;
            String priority = node.has("priority") ? node.get("priority").asText(null) : null;

            if (orderId == null || customer == null || amount == null) {
                log.error("[CONSUMER-V3] BREAKING CHANGE ! Champs manquants dans l'event {}.", version);
                log.error("[CONSUMER-V3]   orderId={}, customer={}, amount={}", orderId, customer, amount);

                Map<String, Object> result = new LinkedHashMap<>();
                result.put("status", "ECHEC");
                result.put("schema", version);
                result.put("reason", "Champs obligatoires manquants (customer ou amount)");
                result.put("json", json);
                processedEvents.add(result);
                return;
            }

            // Gerer les champs optionnels avec des valeurs par defaut
            if (email == null) {
                log.info("[CONSUMER-V3] Champ 'email' absent (ancien schema) -> default: N/A");
            }
            if (priority == null) {
                log.info("[CONSUMER-V3] Champ 'priority' absent (ancien schema) -> default: MEDIUM");
                priority = "MEDIUM";  // valeur par defaut
            }

            log.info("[CONSUMER-V3] Traite : orderId={}, customer={}, amount={}, email={}, priority={}",
                    orderId, customer, amount, email, priority);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "OK");
            result.put("schema", version);
            result.put("orderId", orderId);
            result.put("customer", customer);
            result.put("amount", amount);
            result.put("email", email != null ? email : "N/A (default)");
            result.put("priority", priority);
            processedEvents.add(result);

        } catch (Exception e) {
            log.error("[CONSUMER-V3] Erreur de parsing : {}", e.getMessage());
        }
    }

    public List<Map<String, Object>> getProcessedEvents() {
        return processedEvents;
    }
}
