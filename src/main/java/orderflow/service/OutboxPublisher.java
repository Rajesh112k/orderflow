package orderflow.service;

import lombok.RequiredArgsConstructor;
import orderflow.model.OutboxEvent;
import orderflow.model.OutboxStatus;
import orderflow.repository.OutboxEventRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@Profile("!test")
public class OutboxPublisher {

    private static final String TOPIC = "order-events";
    private static final int MAX_RETRIES = 5;

    private final OutboxClaimService outboxClaimService;
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxStatusService outboxStatusService;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxClaimService
                        .claimPendingEvents();

        for (OutboxEvent event : events) {
            publishEvent(event);
        }
    }

    private void publishEvent(
            OutboxEvent event) {

        String key =
                event.getAggregateId()
                        .toString();

        kafkaTemplate.send(
                TOPIC,
                key,
                event.getPayload()
        ).whenComplete(
                (result, exception) -> {

                    if (exception != null) {

                        outboxStatusService
                                .markFailedAttempt(
                                        event.getId()
                                );

                        System.out.println(
                                "Kafka publish failed: "
                                        + event.getId()
                        );

                        return;
                    }

                    outboxStatusService
                            .markPublished(
                                    event.getId()
                            );

                    System.out.println(
                            "Kafka publish succeeded: "
                                    + event.getId()
                    );
                }
        );
    }

    private void handleSuccess(
            OutboxEvent event) {

        event.setStatus(
                OutboxStatus.PUBLISHED
        );

        event.setLastAttemptAt(
                Instant.now()
        );

        event.setProcessingStartedAt(null);

        outboxEventRepository.save(event);

        System.out.println(
                "Kafka event published successfully: "
                        + event.getId()
        );
    }

    private void handleFailure(
            OutboxEvent event) {

        int newRetryCount =
                event.getRetryCount() + 1;

        event.setRetryCount(
                newRetryCount
        );

        event.setLastAttemptAt(
                Instant.now()
        );

        event.setProcessingStartedAt(null);

        if (newRetryCount >= MAX_RETRIES) {

            event.setStatus(
                    OutboxStatus.FAILED
            );

            System.out.println(
                    "Outbox event permanently failed: "
                            + event.getId()
                            + ", retry count: "
                            + newRetryCount
            );

        } else {

            event.setStatus(
                    OutboxStatus.PENDING
            );

            System.out.println(
                    "Kafka publish failed. Event: "
                            + event.getId()
                            + ", retry count: "
                            + newRetryCount
            );
        }

        outboxEventRepository.save(event);
    }



    private long calculateBackoffSeconds(
            int retryCount) {

        int exponent =
                Math.max(
                        0,
                        retryCount - 1
                );

        long delay =
                5L * (1L << exponent);

        return Math.min(
                delay,
                60L
        );
    }
}