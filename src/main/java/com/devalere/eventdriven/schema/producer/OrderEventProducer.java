package com.devalere.eventdriven.schema.producer;

import com.devalere.eventdriven.schema.config.KafkaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Producer qui publie des events en differentes versions de schema.
 * Utilise du JSON brut (String) pour montrer concretement ce qui circule sur Kafka.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Publie un event V1 : { orderId, customer, amount }
     */
    public void publishV1(Long orderId, String customer, double amount) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("schemaVersion", "V1");
        event.put("orderId", orderId);
        event.put("customer", customer);
        event.put("amount", amount);

        send(orderId, event, "V1");
    }

    /**
     * Publie un event V2 : V1 + email (optionnel)
     */
    public void publishV2(Long orderId, String customer, double amount, String email) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("schemaVersion", "V2");
        event.put("orderId", orderId);
        event.put("customer", customer);
        event.put("amount", amount);
        event.put("email", email);

        send(orderId, event, "V2");
    }

    /**
     * Publie un event V3 : V2 + priority (optionnel)
     */
    public void publishV3(Long orderId, String customer, double amount, String email, String priority) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("schemaVersion", "V3");
        event.put("orderId", orderId);
        event.put("customer", customer);
        event.put("amount", amount);
        event.put("email", email);
        event.put("priority", priority);

        send(orderId, event, "V3");
    }

    /**
     * Publie un event BREAKING : renomme "customer" -> "customerName", supprime "amount"
     * C'est ce qu'il ne faut PAS faire !
     */
    public void publishBreaking(Long orderId, String customerName, String email, String priority, double totalPrice) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("schemaVersion", "BREAKING");
        event.put("orderId", orderId);
        event.put("customerName", customerName);  // RENOMME !
        // "amount" SUPPRIME !
        event.put("email", email);
        event.put("priority", priority);
        event.put("totalPrice", totalPrice);       // NOUVEAU a la place

        send(orderId, event, "BREAKING");
    }

    private void send(Long orderId, Map<String, Object> event, String version) {
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(KafkaConfig.ORDER_TOPIC, String.valueOf(orderId), json);
            log.info("[PRODUCER] Event {} publie : {}", version, json);
        } catch (Exception e) {
            log.error("[PRODUCER] Erreur serialisation : {}", e.getMessage());
        }
    }
}
