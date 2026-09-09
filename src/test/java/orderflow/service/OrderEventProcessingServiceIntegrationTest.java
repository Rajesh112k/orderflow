package orderflow.service;

import orderflow.event.OrderEvent;
import orderflow.repository.ProcessedEventRepository;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("test")
class OrderEventProcessingServiceIntegrationTest {

    @Autowired
    private OrderEventProcessingService processingService;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    @Transactional
    void shouldInsertProcessedEventOnlyOnceForSameEventId() {

        String eventId =
                "duplicate-test-" + System.nanoTime();

        int firstInsert =
                processedEventRepository.insertIfNotExists(
                        eventId,
                        Instant.now()
                );

        int secondInsert =
                processedEventRepository.insertIfNotExists(
                        eventId,
                        Instant.now()
                );

        assertEquals(
                1,
                firstInsert
        );

        assertEquals(
                0,
                secondInsert
        );
    }

    @Test
    void shouldIgnoreDuplicateEventWhenSameEventIdIsProcessedTwice() {

        String eventId =
                "duplicate-service-test-" + System.nanoTime();

        OrderEvent event =
                OrderEvent.builder()
                        .eventId(eventId)
                        .eventType("PRODUCT_PURCHASED")
                        .productId(1L)
                        .quantity(1)
                        .occurredAt(Instant.now())
                        .build();


        processingService.process(event);

        processingService.process(event);


        boolean processed =
                processedEventRepository.existsById(eventId);

        assertTrue(processed);
    }

}