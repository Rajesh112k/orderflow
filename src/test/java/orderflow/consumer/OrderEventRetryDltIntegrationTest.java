package orderflow.consumer;

import orderflow.event.OrderEvent;
import orderflow.service.OrderEventProcessingService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

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

                /*
                 * IMPORTANT:
                 *
                 * The integration test must have its own
                 * application consumer group.
                 *
                 * It must not compete with:
                 *
                 * orderflow-consumer
                 *
                 * which may be used by another running
                 * OrderFlow application.
                 */
                "orderflow.kafka.consumer.group-id="
                        + "orderflow-retry-success-main-consumer"
        }
)

@ActiveProfiles("test")
class OrderEventRetryDltIntegrationTest {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    /*
     * We use ApplicationContext to retrieve Spring Kafka's
     * KafkaListenerEndpointRegistry.
     *
     * The registry allows the test to check whether the real
     * application @KafkaListener has actually received a
     * Kafka partition before publishing the test event.
     */
    @Autowired
    private ApplicationContext applicationContext;


    /*
     * Only the business processing service is mocked.
     *
     * Everything else remains real:
     *
     * Kafka
     * KafkaTemplate
     * OrderEventConsumer
     * Jackson
     * DefaultErrorHandler
     * retry mechanism
     * DeadLetterPublishingRecoverer
     */
    @MockitoBean
    private OrderEventProcessingService processingService;


    /*
     * ============================================================
     * DLT TEST CONSUMER FACTORY
     * ============================================================
     *
     * This creates a Kafka consumer used only by this test.
     *
     * Its purpose is to watch:
     *
     * order-events.DLT
     *
     * and verify that our event does NOT reach the DLT after
     * succeeding during retry.
     */
    private ConsumerFactory<String, String>
    createDltConsumerFactory() {

        Map<String, Object> props =
                new HashMap<>();


        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );


        /*
         * Unique DLT consumer group for every test execution.
         *
         * This prevents committed offsets from previous test
         * runs from affecting this run.
         */
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "orderflow-retry-success-dlt-"
                        + UUID.randomUUID()
        );


        /*
         * Start observing the DLT from its current end.
         *
         * We only care about DLT records produced after this
         * test observer starts.
         */
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


        return new DefaultKafkaConsumerFactory<>(
                props
        );
    }


    /*
     * ============================================================
     * WAIT FOR APPLICATION KAFKA LISTENER
     * ============================================================
     *
     * Previously this test used:
     *
     * Thread.sleep(2000)
     *
     * That is not reliable.
     *
     * A slow CI machine may require more than two seconds for:
     *
     * consumer creation
     *      ↓
     * group join
     *      ↓
     * rebalance
     *      ↓
     * partition assignment
     *
     * Therefore we now wait for an ACTUAL partition assignment
     * instead of guessing how long startup takes.
     */
    private void waitForMainKafkaListenerAssignment()
            throws InterruptedException {

        KafkaListenerEndpointRegistry registry =
                applicationContext.getBean(
                        KafkaListenerEndpointRegistry.class
                );


        long deadline =
                System.currentTimeMillis() + 20_000;


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


                /*
                 * Find the application listener consuming:
                 *
                 * order-events
                 */
                boolean orderEventsListener =
                        false;


                for (String topic : topics) {

                    if ("order-events".equals(topic)) {

                        orderEventsListener =
                                true;

                        break;
                    }
                }


                if (!orderEventsListener) {
                    continue;
                }


                /*
                 * OrderEventConsumer uses a
                 * ConcurrentKafkaListenerContainerFactory.
                 *
                 * Therefore the parent listener container may
                 * contain multiple child Kafka consumers.
                 */
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
                     * We require at least one assignment.
                     *
                     * Do NOT require three here.
                     *
                     * Your current local order-events topic has
                     * only one partition.
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

                } else {


                    /*
                     * Fallback if the application listener is
                     * later changed to a non-concurrent
                     * listener container.
                     */
                    if (!listenerContainer
                            .getAssignedPartitions()
                            .isEmpty()) {

                        return;
                    }
                }
            }


            Thread.sleep(100);
        }


        fail(
                "Main order-events Kafka listener did not receive "
                        + "a partition assignment within 20 seconds"
        );
    }


    /*
     * ============================================================
     * WAIT FOR A SPECIFIC EVENT IN DLT
     * ============================================================
     *
     * We do NOT assume that every record observed in the shared
     * DLT belongs to this test.
     *
     * Instead we search specifically for this test's:
     *
     * eventId
     *
     * and
     *
     * Kafka key.
     */
    private boolean waitForEventInDlt(
            BlockingQueue<ConsumerRecord<String, String>> dltRecords,
            String expectedKey,
            String expectedEventId,
            long timeout,
            TimeUnit timeUnit
    ) throws InterruptedException {


        long deadline =
                System.currentTimeMillis()
                        + timeUnit.toMillis(timeout);


        while (System.currentTimeMillis() < deadline) {

            long remaining =
                    deadline
                            - System.currentTimeMillis();


            if (remaining <= 0) {
                break;
            }


            ConsumerRecord<String, String> record =
                    dltRecords.poll(
                            Math.min(
                                    remaining,
                                    500
                            ),
                            TimeUnit.MILLISECONDS
                    );


            if (record == null) {
                continue;
            }


            /*
             * Ignore DLT records belonging to other tests.
             */
            if (
                    expectedKey.equals(
                            record.key()
                    )
                            &&
                            record.value() != null
                            &&
                            record
                                    .value()
                                    .contains(expectedEventId)
            ) {

                return true;
            }
        }


        return false;
    }


    /*
     * ============================================================
     * RETRY SUCCESS TEST
     * ============================================================
     *
     * Expected behavior:
     *
     * Kafka event
     *      ↓
     * attempt #1
     *      ↓
     * RuntimeException
     *      ↓
     * DefaultErrorHandler
     *      ↓
     * retry
     *      ↓
     * attempt #2
     *      ↓
     * SUCCESS
     *      ↓
     * no DLT
     */
    @Test
    void shouldSucceedOnRetryAndNotSendEventToDlt()
            throws Exception {


        BlockingQueue<ConsumerRecord<String, String>> dltRecords =
                new LinkedBlockingQueue<>();


        ConsumerFactory<String, String> dltConsumerFactory =
                createDltConsumerFactory();


        /*
         * Manual observer for:
         *
         * order-events.DLT
         */
        ContainerProperties containerProperties =
                new ContainerProperties(
                        "order-events.DLT"
                );


        KafkaMessageListenerContainer<String, String> dltContainer =
                new KafkaMessageListenerContainer<>(
                        dltConsumerFactory,
                        containerProperties
                );


        /*
         * Whenever the observer receives a DLT record,
         * place it into our thread-safe queue.
         */
        dltContainer.setupMessageListener(
                (MessageListener<String, String>) dltRecords::add
        );


        /*
         * Start the DLT observer before publishing our
         * normal Kafka event.
         */
        dltContainer.start();


        /*
         * ========================================================
         * WAIT FOR DLT OBSERVER
         * ========================================================
         */
        long dltAssignmentDeadline =
                System.currentTimeMillis()
                        + 10_000;


        while (
                dltContainer
                        .getAssignedPartitions()
                        .isEmpty()
                        &&
                        System.currentTimeMillis()
                                < dltAssignmentDeadline
        ) {

            Thread.sleep(100);
        }


        assertFalse(
                dltContainer
                        .getAssignedPartitions()
                        .isEmpty(),
                "DLT consumer did not receive partition assignment"
        );


        try {

            /*
             * ====================================================
             * CREATE UNIQUE TEST EVENT
             * ====================================================
             */
            String eventId =
                    "retry-success-test-"
                            + System.nanoTime();


            String key =
                    "product-456";


            String payload =
                    """
                    {
                      "eventId": "%s",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 456,
                      "quantity": 1,
                      "occurredAt": "2026-09-05T19:00:00Z"
                    }
                    """.formatted(eventId);


            /*
             * Counts processing attempts only for THIS event.
             */
            AtomicInteger attempts =
                    new AtomicInteger(0);


            /*
             * ====================================================
             * MOCK BUSINESS PROCESSING
             * ====================================================
             *
             * Attempt #1:
             *
             * throw RuntimeException
             *
             * Attempt #2:
             *
             * return normally
             */
            doAnswer(invocation -> {

                OrderEvent event =
                        invocation.getArgument(0);


                /*
                 * Ignore unrelated records that might be
                 * consumed from the shared Kafka topic.
                 */
                if (
                        event == null
                                ||
                                !eventId.equals(
                                        event.getEventId()
                                )
                ) {

                    return null;
                }


                int attempt =
                        attempts.incrementAndGet();


                System.out.println(
                        "\nProcessing event "
                                + eventId
                                + " - attempt #"
                                + attempt
                );


                /*
                 * First processing attempt fails.
                 */
                if (attempt == 1) {

                    System.out.println(
                            "Attempt #1 intentionally failing"
                    );


                    throw new RuntimeException(
                            "Simulated temporary processing failure"
                    );
                }


                /*
                 * Second attempt succeeds.
                 */
                System.out.println(
                        "Attempt #"
                                + attempt
                                + " succeeded"
                );


                return null;

            })
                    .when(processingService)
                    .process(
                            any(OrderEvent.class)
                    );


            /*
             * ====================================================
             * WAIT FOR REAL APPLICATION LISTENER
             * ====================================================
             *
             * This replaces:
             *
             * Thread.sleep(2000)
             *
             * We now know the listener actually owns a Kafka
             * partition before publishing the event.
             */
            waitForMainKafkaListenerAssignment();


            System.out.println(
                    "\nSending retry-success event..."
            );


            System.out.println(
                    "Event ID : "
                            + eventId
            );


            System.out.println(
                    "Kafka Key: "
                            + key
            );


            /*
             * ====================================================
             * PUBLISH EVENT
             * ====================================================
             */
            var sendResult =
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


            System.out.println(
                    "Partition: "
                            + sendResult
                            .getRecordMetadata()
                            .partition()
            );


            System.out.println(
                    "Offset   : "
                            + sendResult
                            .getRecordMetadata()
                            .offset()
            );


            /*
             * ====================================================
             * WAIT FOR RETRY SUCCESS
             * ====================================================
             */
            long retryDeadline =
                    System.currentTimeMillis()
                            + 15_000;


            while (
                    attempts.get() < 2
                            &&
                            System.currentTimeMillis()
                                    < retryDeadline
            ) {

                Thread.sleep(100);
            }


            System.out.println(
                    "\nAttempts observed: "
                            + attempts.get()
            );


            /*
             * ====================================================
             * VERIFY EXACTLY TWO ATTEMPTS
             * ====================================================
             *
             * attempt #1 -> failure
             *
             * attempt #2 -> success
             */
            assertEquals(
                    2,
                    attempts.get(),
                    "Expected first processing attempt to fail "
                            + "and second attempt to succeed"
            );


            /*
             * Verify Mockito also observed exactly two calls
             * for THIS event.
             */
            verify(
                    processingService,
                    times(2)
            )
                    .process(
                            argThat(event ->
                                    event != null
                                            &&
                                            eventId.equals(
                                                    event.getEventId()
                                            )
                            )
                    );


            /*
             * ====================================================
             * VERIFY EVENT DID NOT REACH DLT
             * ====================================================
             *
             * Since attempt #2 succeeded, the
             * DeadLetterPublishingRecoverer should never be
             * invoked for this event.
             */
            boolean reachedDlt =
                    waitForEventInDlt(
                            dltRecords,
                            key,
                            eventId,
                            4,
                            TimeUnit.SECONDS
                    );


            assertFalse(
                    reachedDlt,
                    "Event succeeded on retry and should not reach DLT"
            );


            System.out.println(
                    "\n========== RETRY SUCCESS RESULT =========="
            );


            System.out.println(
                    "Event ID            : "
                            + eventId
            );


            System.out.println(
                    "Processing attempts : "
                            + attempts.get()
            );


            System.out.println(
                    "Reached DLT         : "
                            + reachedDlt
            );


            System.out.println(
                    "Result              : SUCCESS ON RETRY"
            );


            System.out.println(
                    "==========================================\n"
            );

        } finally {

            /*
             * Always stop the manually-created DLT observer.
             *
             * This executes even when the test fails.
             */
            dltContainer.stop();
        }
    }
}