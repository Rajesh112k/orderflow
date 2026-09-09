package orderflow.consumer;

import lombok.RequiredArgsConstructor;

import orderflow.event.OrderEvent;
import orderflow.model.FailedEventStatus;
import orderflow.repository.FailedEventRepository;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;


@Component
@RequiredArgsConstructor
public class DltFailureConsumer {

    private final FailedEventRepository failedEventRepository;

    private final ObjectMapper objectMapper;


    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "orderflow-dlt-storage",
            containerFactory =
                    "dltStorageKafkaListenerContainerFactory"
    )
    
    @Transactional
    public void consume(
            ConsumerRecord<String, String> record
    ) {

        System.out.println(
                "\n========== DLT RECORD RECEIVED =========="
        );

        System.out.println(
                "DLT topic     : "
                        + record.topic()
        );

        System.out.println(
                "DLT partition : "
                        + record.partition()
        );

        System.out.println(
                "DLT offset    : "
                        + record.offset()
        );

        System.out.println(
                "Kafka key     : "
                        + record.key()
        );

        System.out.println(
                "Payload       : "
                        + record.value()
        );

        System.out.println(
                "=========================================\n"
        );


        /*
         * =========================================================
         * READ DLT HEADERS
         * =========================================================
         */
        Headers headers =
                record.headers();


        String originalTopic =
                readStringHeader(
                        headers,
                        "kafka_dlt-original-topic"
                );


        Integer originalPartition =
                readIntegerHeader(
                        headers,
                        "kafka_dlt-original-partition"
                );


        Long originalOffset =
                readLongHeader(
                        headers,
                        "kafka_dlt-original-offset"
                );


        String exceptionClass =
                readStringHeader(
                        headers,
                        "kafka_dlt-exception-fqcn"
                );


        String exceptionMessage =
                readStringHeader(
                        headers,
                        "kafka_dlt-exception-message"
                );


        /*
         * =========================================================
         * EXTRACT LOGICAL EVENT ID
         * =========================================================
         */
        String eventId =
                extractEventId(
                        record.value(),
                        originalTopic,
                        originalPartition,
                        originalOffset
                );


        /*
         * =========================================================
         * ATOMIC DATABASE INSERT
         * =========================================================
         */
        int inserted =
                failedEventRepository
                        .insertIfNotExists(
                                eventId,
                                originalTopic,
                                originalPartition,
                                originalOffset,
                                record.key(),
                                record.value(),
                                exceptionClass,
                                exceptionMessage,
                                FailedEventStatus
                                        .FAILED
                                        .name(),
                                0,
                                Instant.now(),
                                null
                        );


        /*
         * insertIfNotExists returns:
         *
         * 1 = inserted
         *
         * 0 = duplicate
         */
        if (inserted == 1) {

            System.out.println(
                    "\n========== FAILED EVENT STORED =========="
            );

            System.out.println(
                    "Event ID           : "
                            + eventId
            );

            System.out.println(
                    "Original topic     : "
                            + originalTopic
            );

            System.out.println(
                    "Original partition : "
                            + originalPartition
            );

            System.out.println(
                    "Original offset    : "
                            + originalOffset
            );

            System.out.println(
                    "Status             : "
                            + FailedEventStatus.FAILED
            );

            System.out.println(
                    "=========================================\n"
            );

        } else {

            System.out.println(
                    "\n========== DUPLICATE DLT RECORD =========="
            );

            System.out.println(
                    "Already stored:"
            );

            System.out.println(
                    originalTopic
                            + "-"
                            + originalPartition
                            + "-"
                            + originalOffset
            );

            System.out.println(
                    "==========================================\n"
            );
        }
    }


    /*
     * =============================================================
     * EXTRACT EVENT ID
     * =============================================================
     */
    private String extractEventId(
            String payload,
            String originalTopic,
            Integer originalPartition,
            Long originalOffset
    ) {

        try {

            OrderEvent event =
                    objectMapper.readValue(
                            payload,
                            OrderEvent.class
                    );


            if (
                    event.getEventId() != null
                            &&
                            !event
                                    .getEventId()
                                    .isBlank()
            ) {

                return event.getEventId();
            }

        } catch (Exception ignored) {

            /*
             * Malformed JSON is allowed to reach the DLT.
             *
             * The DLT storage consumer must still be able
             * to store that failure.
             */
        }


        /*
         * Fallback identifier for malformed events.
         *
         * Example:
         *
         * unknown-order-events-2-45
         */
        return "unknown-"
                + originalTopic
                + "-"
                + originalPartition
                + "-"
                + originalOffset;
    }


    /*
     * =============================================================
     * STRING HEADER
     * =============================================================
     */
    private String readStringHeader(
            Headers headers,
            String name
    ) {

        Header header =
                headers.lastHeader(
                        name
                );


        if (
                header == null
                        ||
                        header.value() == null
        ) {

            return null;
        }


        return new String(
                header.value(),
                StandardCharsets.UTF_8
        );
    }


    /*
     * =============================================================
     * INTEGER HEADER
     * =============================================================
     */
    private Integer readIntegerHeader(
            Headers headers,
            String name
    ) {

        Header header =
                headers.lastHeader(
                        name
                );


        if (
                header == null
                        ||
                        header.value() == null
        ) {

            return null;
        }


        byte[] value =
                header.value();


        if (
                value.length
                        != Integer.BYTES
        ) {

            return null;
        }


        return ByteBuffer
                .wrap(value)
                .getInt();
    }


    /*
     * =============================================================
     * LONG HEADER
     * =============================================================
     */
    private Long readLongHeader(
            Headers headers,
            String name
    ) {

        Header header =
                headers.lastHeader(
                        name
                );


        if (
                header == null
                        ||
                        header.value() == null
        ) {

            return null;
        }


        byte[] value =
                header.value();


        if (
                value.length
                        != Long.BYTES
        ) {

            return null;
        }


        return ByteBuffer
                .wrap(value)
                .getLong();
    }
}