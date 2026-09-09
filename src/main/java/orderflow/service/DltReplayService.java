package orderflow.service;

import lombok.RequiredArgsConstructor;

import orderflow.model.FailedEvent;
import orderflow.repository.FailedEventRepository;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;


@Service
@RequiredArgsConstructor
public class DltReplayService {

    private static final String ORIGINAL_TOPIC =
            "order-events";


    private final KafkaTemplate<String, String>
            kafkaTemplate;

    private final FailedEventRepository
            failedEventRepository;

    private final FailedEventReplayStateService
            replayStateService;


    public ReplayResult replay(
            Long failedEventId
    ) {

        /*
         * ========================================================
         * STEP 1: LOAD EVENT
         * ========================================================
         */
        FailedEvent failedEvent =
                failedEventRepository
                        .findById(
                                failedEventId
                        )
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Failed event not found: "
                                                        + failedEventId
                                        )
                        );


        /*
         * ========================================================
         * STEP 2: ATOMIC CLAIM
         * ========================================================
         *
         * Only one concurrent caller can successfully change:
         *
         * FAILED → REPLAYING
         */
        boolean claimed =
                replayStateService.claim(
                        failedEventId
                );


        if (!claimed) {

            return new ReplayResult(
                    false,
                    failedEventId,
                    null,
                    null,
                    null,
                    "Failed event is not eligible for replay or is already being replayed"
            );
        }


        /*
         * ========================================================
         * STEP 3: PUBLISH ORIGINAL PAYLOAD
         * ========================================================
         */
        try {

            var sendResult =
                    kafkaTemplate
                            .send(
                                    ORIGINAL_TOPIC,
                                    failedEvent
                                            .getOriginalKey(),
                                    failedEvent
                                            .getPayload()
                            )
                            .get(
                                    10,
                                    TimeUnit.SECONDS
                            );


            int partition =
                    sendResult
                            .getRecordMetadata()
                            .partition();


            long offset =
                    sendResult
                            .getRecordMetadata()
                            .offset();


            /*
             * Kafka acknowledged publication.
             *
             * REPLAYING → REPLAYED
             */
            replayStateService
                    .markSucceeded(
                            failedEventId
                    );


            return new ReplayResult(
                    true,
                    failedEventId,
                    ORIGINAL_TOPIC,
                    partition,
                    offset,
                    null
            );

        } catch (Exception exception) {

            /*
             * Kafka publication failed.
             *
             * Make the event available for replay again.
             *
             * REPLAYING → FAILED
             */
            replayStateService
                    .releaseClaim(
                            failedEventId
                    );


            return new ReplayResult(
                    false,
                    failedEventId,
                    ORIGINAL_TOPIC,
                    null,
                    null,
                    exception.getMessage()
            );
        }
    }


    public record ReplayResult(
            boolean success,
            Long failedEventId,
            String topic,
            Integer partition,
            Long offset,
            String error
    ) {
    }
}