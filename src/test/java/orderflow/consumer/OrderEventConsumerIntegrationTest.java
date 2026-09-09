package orderflow.consumer;

import orderflow.repository.ProcessedEventRepository;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true"
        }
)
@ActiveProfiles("test")
class OrderEventConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void shouldProcessDuplicateKafkaEventOnlyOnce()
            throws Exception {

        String eventId =
                "kafka-duplicate-" + System.nanoTime();

        String key =
                "product-123";

        String payload =
                """
                {
                  "eventId": "%s",
                  "eventType": "PRODUCT_PURCHASED",
                  "productId": 123,
                  "quantity": 1,
                  "occurredAt": "2026-09-04T19:00:00Z"
                }
                """.formatted(eventId);


        kafkaTemplate
                .send(
                        "order-events",
                        key,
                        payload
                )
                .get(10, TimeUnit.SECONDS);


        kafkaTemplate
                .send(
                        "order-events",
                        key,
                        payload
                )
                .get(10, TimeUnit.SECONDS);


        var firstResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                payload
                        )
                        .get(10, TimeUnit.SECONDS);

        var secondResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                payload
                        )
                        .get(10, TimeUnit.SECONDS);


        int firstPartition =
                firstResult.getRecordMetadata().partition();

        long firstOffset =
                firstResult.getRecordMetadata().offset();

        int secondPartition =
                secondResult.getRecordMetadata().partition();

        long secondOffset =
                secondResult.getRecordMetadata().offset();


        assertEquals(
                firstPartition,
                secondPartition
        );

        assertTrue(
                secondOffset > firstOffset
        );
        long deadline =
                System.currentTimeMillis() + 15_000;

        while (
                !processedEventRepository.existsById(eventId)
                        &&
                        System.currentTimeMillis() < deadline
        ) {
            Thread.sleep(100);
        }


        assertTrue(
                processedEventRepository.existsById(eventId),
                "Expected Kafka event to be processed"
        );
    }
}