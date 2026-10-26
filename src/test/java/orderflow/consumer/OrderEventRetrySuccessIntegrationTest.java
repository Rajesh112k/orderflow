
package orderflow.consumer;

import orderflow.event.OrderEvent;
import orderflow.service.OrderEventProcessingService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true",
                "spring.kafka.consumer.auto-offset-reset=latest",
                "orderflow.kafka.consumer.group-id=orderflow-retry-success-main-consumer"
        }
)
@ActiveProfiles("test")
class OrderEventRetrySuccessIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerRegistry;

    @MockitoBean
    private OrderEventProcessingService processingService;

    private ConsumerFactory<String, String> createDltConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "orderflow-retry-success-dlt-" + UUID.randomUUID()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        return new DefaultKafkaConsumerFactory<>(props);
    }

    private void waitForMainConsumerAssignment()
            throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + 30_000;

        boolean consumerReady = false;

        while (System.currentTimeMillis() < deadline) {

            consumerReady = kafkaListenerRegistry
                    .getListenerContainers()
                    .stream()
                    .filter(MessageListenerContainer::isRunning)
                    .anyMatch(container -> {

                        String[] topics =
                                container.getContainerProperties()
                                        .getTopics();

                        return topics != null
                                && Arrays.asList(topics)
                                .contains("order-events")
                                && container.getAssignedPartitions() != null
                                && !container.getAssignedPartitions()
                                .isEmpty();
                    });

            if (consumerReady) {
                break;
            }

            Thread.sleep(100);
        }

        assertTrue(
                consumerReady,
                "Main order-events Kafka consumer did not "
                        + "receive partition assignments"
        );

        System.out.println(
                "Main Kafka consumer has partition assignments"
        );
    }

    private void waitForDltConsumerAssignment(
            KafkaMessageListenerContainer<String, String> container
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + 30_000;

        while (
                (container.getAssignedPartitions() == null
                        || container.getAssignedPartitions().isEmpty())
                        && System.currentTimeMillis() < deadline
        ) {
            Thread.sleep(100);
        }

        assertNotNull(
                container.getAssignedPartitions(),
                "DLT consumer assignment is null"
        );

        assertFalse(
                container.getAssignedPartitions().isEmpty(),
                "DLT consumer did not receive partition assignment"
        );

        System.out.println(
                "DLT consumer has partition assignments"
        );
    }

    private boolean waitForEventInDlt(
            BlockingQueue<ConsumerRecord<String, String>> records,
            String key,
            String eventId
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + 5_000;

        while (System.currentTimeMillis() < deadline) {

            ConsumerRecord<String, String> record =
                    records.poll(100, TimeUnit.MILLISECONDS);

            if (record == null) {
                continue;
            }

            boolean matchingKey =
                    key.equals(record.key());

            boolean matchingEvent =
                    record.value() != null
                            && record.value().contains(eventId);

            if (matchingKey && matchingEvent) {
                return true;
            }
        }

        return false;
    }

    @Test
    void shouldSucceedOnRetryAndNotSendEventToDlt()
            throws Exception {

        BlockingQueue<ConsumerRecord<String, String>> dltRecords =
                new LinkedBlockingQueue<>();

        ConsumerFactory<String, String> dltConsumerFactory =
                createDltConsumerFactory();

        ContainerProperties containerProperties =
                new ContainerProperties("order-events.DLT");

        KafkaMessageListenerContainer<String, String> dltContainer =
                new KafkaMessageListenerContainer<>(
                        dltConsumerFactory,
                        containerProperties
                );

        dltContainer.setupMessageListener(
                (MessageListener<String, String>) dltRecords::add
        );

        dltContainer.start();

        try {

            // Ensure the DLT consumer is ready.
            waitForDltConsumerAssignment(dltContainer);

            // Ensure the main application consumer is ready.
            waitForMainConsumerAssignment();

            String eventId =
                    "retry-success-test-" + UUID.randomUUID();

            String key = "product-456";

            String payload = """
                    {
                      "eventId": "%s",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 456,
                      "quantity": 1,
                      "occurredAt": "2026-09-05T19:00:00Z"
                    }
                    """.formatted(eventId);

            AtomicInteger attempts = new AtomicInteger(0);

            doAnswer(invocation -> {

                OrderEvent event = invocation.getArgument(0);

                if (event == null
                        || !eventId.equals(event.getEventId())) {
                    return null;
                }

                int attempt = attempts.incrementAndGet();

                System.out.println(
                        "Processing event " + eventId
                                + " - attempt #" + attempt
                );

                if (attempt == 1) {

                    System.out.println(
                            "Attempt #1 intentionally failing"
                    );

                    throw new RuntimeException(
                            "Simulated temporary processing failure"
                    );
                }

                System.out.println(
                        "Attempt #" + attempt + " succeeded"
                );

                return null;

            }).when(processingService)
                    .process(any(OrderEvent.class));

            System.out.println(
                    "Sending retry-success event: " + eventId
            );

            var sendResult = kafkaTemplate
                    .send("order-events", key, payload)
                    .get(10, TimeUnit.SECONDS);

            System.out.println(
                    "Partition: "
                            + sendResult.getRecordMetadata().partition()
            );

            System.out.println(
                    "Offset: "
                            + sendResult.getRecordMetadata().offset()
            );

            long retryDeadline =
                    System.currentTimeMillis() + 30_000;

            while (
                    attempts.get() < 2
                            && System.currentTimeMillis() < retryDeadline
            ) {
                Thread.sleep(100);
            }

            System.out.println(
                    "Attempts observed: " + attempts.get()
            );

            assertEquals(
                    2,
                    attempts.get(),
                    "Expected first processing attempt to fail "
                            + "and second attempt to succeed"
            );

            verify(
                    processingService,
                    times(2)
            ).process(
                    argThat(event ->
                            event != null
                                    && eventId.equals(event.getEventId())
                    )
            );

            boolean reachedDlt = waitForEventInDlt(
                    dltRecords,
                    key,
                    eventId
            );

            assertFalse(
                    reachedDlt,
                    "Event should not reach DLT after successful retry"
            );

            System.out.println(
                    "Retry succeeded and event was not sent to DLT"
            );

        } finally {

            dltContainer.stop();

            System.out.println(
                    "Temporary DLT consumer stopped"
            );
        }
    }
}
