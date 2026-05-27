package com.devalere.eventdriven.schema.controller;

import com.devalere.eventdriven.schema.consumer.V1Consumer;
import com.devalere.eventdriven.schema.consumer.V3Consumer;
import com.devalere.eventdriven.schema.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class SchemaEvolutionController {

    private final OrderEventProducer producer;
    private final V1Consumer v1Consumer;
    private final V3Consumer v3Consumer;

    // =============================================
    //  PUBLIER DES EVENTS EN DIFFERENTES VERSIONS
    // =============================================

    /**
     * Publie un event V1 : { orderId, customer, amount }
     */
    @PostMapping("/publish/v1")
    public ResponseEntity<Map<String, String>> publishV1(
            @RequestParam(defaultValue = "1") Long orderId,
            @RequestParam(defaultValue = "Devalere") String customer,
            @RequestParam(defaultValue = "99.99") double amount) {

        producer.publishV1(orderId, customer, amount);

        Map<String, String> result = new LinkedHashMap<>();
        result.put("schema", "V1");
        result.put("fields", "orderId, customer, amount");
        result.put("status", "Publie sur Kafka");
        return ResponseEntity.ok(result);
    }

    /**
     * Publie un event V2 : V1 + email
     */
    @PostMapping("/publish/v2")
    public ResponseEntity<Map<String, String>> publishV2(
            @RequestParam(defaultValue = "2") Long orderId,
            @RequestParam(defaultValue = "Alice") String customer,
            @RequestParam(defaultValue = "149.99") double amount,
            @RequestParam(defaultValue = "alice@mail.com") String email) {

        producer.publishV2(orderId, customer, amount, email);

        Map<String, String> result = new LinkedHashMap<>();
        result.put("schema", "V2");
        result.put("fields", "orderId, customer, amount, email (NEW)");
        result.put("status", "Publie sur Kafka");
        return ResponseEntity.ok(result);
    }

    /**
     * Publie un event V3 : V2 + priority
     */
    @PostMapping("/publish/v3")
    public ResponseEntity<Map<String, String>> publishV3(
            @RequestParam(defaultValue = "3") Long orderId,
            @RequestParam(defaultValue = "Bob") String customer,
            @RequestParam(defaultValue = "299.99") double amount,
            @RequestParam(defaultValue = "bob@mail.com") String email,
            @RequestParam(defaultValue = "HIGH") String priority) {

        producer.publishV3(orderId, customer, amount, email, priority);

        Map<String, String> result = new LinkedHashMap<>();
        result.put("schema", "V3");
        result.put("fields", "orderId, customer, amount, email, priority (NEW)");
        result.put("status", "Publie sur Kafka");
        return ResponseEntity.ok(result);
    }

    /**
     * Publie un event BREAKING : renomme customer, supprime amount
     * Pour montrer ce qu'il ne faut PAS faire.
     */
    @PostMapping("/publish/breaking")
    public ResponseEntity<Map<String, String>> publishBreaking(
            @RequestParam(defaultValue = "4") Long orderId,
            @RequestParam(defaultValue = "Charlie") String customerName,
            @RequestParam(defaultValue = "charlie@mail.com") String email,
            @RequestParam(defaultValue = "LOW") String priority,
            @RequestParam(defaultValue = "199.99") double totalPrice) {

        producer.publishBreaking(orderId, customerName, email, priority, totalPrice);

        Map<String, String> result = new LinkedHashMap<>();
        result.put("schema", "BREAKING");
        result.put("warning", "customer renomme en customerName, amount supprime !");
        result.put("expected", "Les consumers V1 et V3 vont echouer (champs manquants)");
        result.put("status", "Publie sur Kafka");
        return ResponseEntity.ok(result);
    }

    // =============================================
    //  DEMO COMPLETE
    // =============================================

    /**
     * Demo : publie 1 event de chaque version.
     * Montre la compatibilite backward/forward et le breaking change.
     */
    @PostMapping("/demo")
    public ResponseEntity<Map<String, String>> demo() {
        log.info("=== DEMO SCHEMA EVOLUTION ===");

        producer.publishV1(1L, "Devalere", 99.99);
        producer.publishV2(2L, "Alice", 149.99, "alice@mail.com");
        producer.publishV3(3L, "Bob", 299.99, "bob@mail.com", "HIGH");
        producer.publishBreaking(4L, "Charlie", "charlie@mail.com", "LOW", 199.99);

        Map<String, String> result = new LinkedHashMap<>();
        result.put("event_1", "V1 : { orderId=1, customer=Devalere, amount=99.99 }");
        result.put("event_2", "V2 : + email=alice@mail.com");
        result.put("event_3", "V3 : + priority=HIGH");
        result.put("event_4", "BREAKING : customer->customerName, amount supprime !");
        result.put("consumer_v1", "V1 lit V1(OK), V2(OK ignore email), V3(OK ignore email+priority), BREAKING(ECHEC !)");
        result.put("consumer_v3", "V3 lit V1(OK default), V2(OK default priority), V3(OK), BREAKING(ECHEC !)");
        result.put("message", "GET /api/results pour voir le detail apres 2-3 secondes");
        return ResponseEntity.ok(result);
    }

    // =============================================
    //  VOIR LES RESULTATS
    // =============================================

    /**
     * Compare ce que chaque consumer a vu.
     */
    @GetMapping("/results")
    public ResponseEntity<Map<String, Object>> results() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("consumer_v1_events", v1Consumer.getProcessedEvents());
        results.put("consumer_v3_events", v3Consumer.getProcessedEvents());
        results.put("explication", "V1 et V3 traitent les events compatibles. Le BREAKING change casse les deux.");
        return ResponseEntity.ok(results);
    }

    @GetMapping("/results/v1")
    public ResponseEntity<?> resultsV1() {
        return ResponseEntity.ok(v1Consumer.getProcessedEvents());
    }

    @GetMapping("/results/v3")
    public ResponseEntity<?> resultsV3() {
        return ResponseEntity.ok(v3Consumer.getProcessedEvents());
    }
}
