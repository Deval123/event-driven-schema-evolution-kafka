package com.devalere.eventdriven.schema;

import com.devalere.eventdriven.schema.consumer.V1Consumer;
import com.devalere.eventdriven.schema.consumer.V3Consumer;
import com.devalere.eventdriven.schema.producer.OrderEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-events"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9192",
                "port=9192"
        }
)
@DirtiesContext
class SchemaEvolutionApplicationTests {

    @Autowired
    private OrderEventProducer producer;

    @Autowired
    private V1Consumer v1Consumer;

    @Autowired
    private V3Consumer v3Consumer;

    @Test
    void contextLoads() {
        assertThat(producer).isNotNull();
        assertThat(v1Consumer).isNotNull();
        assertThat(v3Consumer).isNotNull();
    }

    @Test
    void shouldPublishAndConsumeV1Event() throws Exception {
        producer.publishV1(100L, "TestUser", 50.0);
        Thread.sleep(3000);

        assertThat(v1Consumer.getProcessedEvents()).anyMatch(e ->
                "OK".equals(e.get("status")) && "V1".equals(e.get("schema"))
        );

        assertThat(v3Consumer.getProcessedEvents()).anyMatch(e ->
                "OK".equals(e.get("status")) && "V1".equals(e.get("schema"))
        );
    }

    @Test
    void shouldPublishV2AndV1IgnoresUnknownFields() throws Exception {
        producer.publishV2(200L, "Alice", 100.0, "alice@test.com");
        Thread.sleep(3000);

        // V1 consumer traite OK et ignore le champ email (FORWARD compatible)
        assertThat(v1Consumer.getProcessedEvents()).anyMatch(e ->
                "OK".equals(e.get("status")) && "V2".equals(e.get("schema"))
        );

        // V3 consumer traite OK avec email present
        assertThat(v3Consumer.getProcessedEvents()).anyMatch(e ->
                "OK".equals(e.get("status")) && "V2".equals(e.get("schema"))
        );
    }

    @Test
    void shouldPublishV3AndV1IgnoresNewFields() throws Exception {
        producer.publishV3(300L, "Bob", 200.0, "bob@test.com", "HIGH");
        Thread.sleep(3000);

        // V1 consumer ignore email + priority (FORWARD compatible)
        assertThat(v1Consumer.getProcessedEvents()).anyMatch(e ->
                "OK".equals(e.get("status")) && "V3".equals(e.get("schema"))
        );

        // V3 consumer traite tout parfaitement
        assertThat(v3Consumer.getProcessedEvents()).anyMatch(e ->
                "OK".equals(e.get("status")) && "V3".equals(e.get("schema"))
        );
    }

    @Test
    void shouldDetectBreakingChange() throws Exception {
        producer.publishBreaking(400L, "Charlie", "charlie@test.com", "LOW", 150.0);
        Thread.sleep(3000);

        // V1 consumer detecte le BREAKING CHANGE (customer manquant)
        assertThat(v1Consumer.getProcessedEvents()).anyMatch(e ->
                "ECHEC".equals(e.get("status")) && "BREAKING".equals(e.get("schema"))
        );

        // V3 consumer detecte aussi le BREAKING CHANGE
        assertThat(v3Consumer.getProcessedEvents()).anyMatch(e ->
                "ECHEC".equals(e.get("status")) && "BREAKING".equals(e.get("schema"))
        );
    }
}
