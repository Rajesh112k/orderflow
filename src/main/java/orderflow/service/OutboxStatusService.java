package orderflow.service;

import lombok.RequiredArgsConstructor;
import orderflow.model.OutboxEvent;
import orderflow.model.OutboxStatus;
import orderflow.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class OutboxStatusService {

    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public void markPublished(String eventId) {

        OutboxEvent event =
                outboxEventRepository.findById(eventId)
                        .orElseThrow();

        event.setStatus(
                OutboxStatus.PUBLISHED
        );

        event.setLastAttemptAt(
                Instant.now()
        );

        event.setProcessingStartedAt(null);
    }

    @Transactional
    public void markFailedAttempt(String eventId) {

        OutboxEvent event =
                outboxEventRepository.findById(eventId)
                        .orElseThrow();

        int newRetryCount =
                event.getRetryCount() + 1;

        Instant now = Instant.now();

        event.setRetryCount(newRetryCount);
        event.setLastAttemptAt(now);
        event.setProcessingStartedAt(null);

        if (newRetryCount >= MAX_RETRIES) {

            event.setStatus(
                    OutboxStatus.FAILED
            );

            return;
        }

        long delaySeconds =
                calculateBackoffSeconds(
                        newRetryCount
                );

        event.setStatus(
                OutboxStatus.PENDING
        );

        event.setNextAttemptAt(
                now.plusSeconds(delaySeconds)
        );
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