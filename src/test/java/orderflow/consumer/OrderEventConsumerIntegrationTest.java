package orderflow.consumer;

import orderflow.repository.ProcessedEventRepository;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true",
                "spring.kafka.consumer.auto-offset-reset=latest",

                /*
                 * IMPORTANT:
                 *
                 * Give this integration test its own application
                 * consumer group.
                 *
                 * Otherwise it falls back to:
                 *
                 * orderflow-consumer
                 *
                 * and can compete with other OrderFlow consumers.
                 */
                "orderflow.kafka.consumer.group-id="
                        + "orderflow-consumer-integration-test"
        }
)
@ActiveProfiles("test")
class OrderEventConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private ApplicationContext applicationContext;


    /*
     * Wait until the real application's order-events
     * Kafka listener owns at least one partition.
     *
     * This removes the startup race that appeared in CI.
     */
    private void waitForMainKafkaListenerAssignment()
            throws InterruptedException {

        KafkaListenerEndpointRegistry registry =
                applicationContext.getBean(
                        KafkaListenerEndpointRegistry.class
                );

        long deadline =
                System.currentTimeMillis()
                        + 20_000;


        while (System.currentTimeMillis() < deadline) {

            for (MessageListenerContainer listenerContainer :
                    registry.getListenerContainers()) {

                String[] topics =
                        listenerContainer
                                .getContainerProperties()
                                .getTopics();

                if (topics == null) {
                    continue;
                }


                boolean orderEventsListener =
                        false;

                for (String topic : topics) {

                    if ("order-events".equals(topic)) {

                        orderEventsListener = true;
                        break;
                    }
                }


                if (!orderEventsListener) {
                    continue;
                }


                if (listenerContainer
                        instanceof ConcurrentMessageListenerContainer<?, ?>
                        concurrentContainer) {

                    int totalAssigned =
                            concurrentContainer
                                    .getContainers()
                                    .stream()
                                    .mapToInt(
                                            child ->
                                                    child
                                                            .getAssignedPartitions()
                                                            .size()
                                    )
                                    .sum();


                    /*
                     * We only require at least one assignment.
                     *
                     * This works with both:
                     *
                     * local Kafka -> currently 1 partition
                     * CI Kafka    -> 3 partitions
                     */
                    if (totalAssigned > 0) {

                        System.out.println(
                                "\nMain order-events listener ready. "
                                        + "Assigned partitions: "
                                        + totalAssigned
                                        + "\n"
                        );

                        return;
                    }

                } else if (
                        !listenerContainer
                                .getAssignedPartitions()
                                .isEmpty()
                ) {

                    return;
                }
            }


            Thread.sleep(100);
        }


        fail(
                "Main order-events Kafka listener did not receive "
                        + "a partition assignment within 20 seconds"
        );
    }


    @Test
    void shouldProcessDuplicateKafkaEventOnlyOnce()
            throws Exception {

        String eventId =
                "kafka-duplicate-"
                        + System.nanoTime();

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


        /*
         * Make sure the application's Kafka listener is
         * actually ready BEFORE producing either record.
         */
        waitForMainKafkaListenerAssignment();


        /*
         * ========================================================
         * FIRST COPY
         * ========================================================
         */
        var firstResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                payload
                        )
                        .get(
                                10,
                                TimeUnit.SECONDS
                        );


        /*
         * ========================================================
         * DUPLICATE COPY
         * ========================================================
         *
         * Same:
         *
         * eventId
         * key
         * payload
         *
         * but this is a separate Kafka record.
         */
        var secondResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                payload
                        )
                        .get(
                                10,
                                TimeUnit.SECONDS
                        );


        int firstPartition =
                firstResult
                        .getRecordMetadata()
                        .partition();

        long firstOffset =
                firstResult
                        .getRecordMetadata()
                        .offset();


        int secondPartition =
                secondResult
                        .getRecordMetadata()
                        .partition();

        long secondOffset =
                secondResult
                        .getRecordMetadata()
                        .offset();


        /*
         * Same Kafka key should map both records
         * to the same partition.
         */
        assertEquals(
                firstPartition,
                secondPartition,
                "Duplicate events should be in the same partition"
        );


        /*
         * They are still two physical Kafka records,
         * so the second must have a later offset.
         */
        assertTrue(
                secondOffset > firstOffset,
                "Second Kafka record should have a later offset"
        );


        System.out.println(
                "\n========== DUPLICATE EVENT =========="
        );

        System.out.println(
                "Event ID         : "
                        + eventId
        );

        System.out.println(
                "First partition  : "
                        + firstPartition
        );

        System.out.println(
                "First offset     : "
                        + firstOffset
        );

        System.out.println(
                "Second partition : "
                        + secondPartition
        );

        System.out.println(
                "Second offset    : "
                        + secondOffset
        );

        System.out.println(
                "=====================================\n"
        );


        /*
         * Wait until the consumer records this logical
         * event in processed_events.
         */
        long deadline =
                System.currentTimeMillis()
                        + 15_000;


        while (
                !processedEventRepository
                        .existsById(eventId)
                        &&
                        System.currentTimeMillis()
                                < deadline
        ) {

            Thread.sleep(100);
        }


        assertTrue(
                processedEventRepository.existsById(eventId),
                "Expected Kafka event to be processed"
        );


        /*
         * The processed_events primary key / unique eventId
         * is what gives us idempotent processing.
         *
         * Even though Kafka received two physical records,
         * there should only be one processed-event marker
         * for this logical eventId.
         */
        assertEquals(
                1,
                processedEventRepository
                        .findAll()
                        .stream()
                        .filter(
                                processedEvent ->
                                        eventId.equals(
                                                processedEvent.getEventId()
                                        )
                        )
                        .count(),
                "Duplicate Kafka event should be recorded only once"
        );


        System.out.println(
                "Duplicate event processed idempotently: YES"
        );
    }
}