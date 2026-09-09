package orderflow.service;

import lombok.RequiredArgsConstructor;
import orderflow.model.OutboxEvent;
import orderflow.model.OutboxStatus;
import orderflow.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OutboxClaimService {

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public List<OutboxEvent> claimPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository
                        .findReadyEventsForUpdate();

        Instant now = Instant.now();

        for (OutboxEvent event : events) {

            event.setStatus(
                    OutboxStatus.PROCESSING
            );

            event.setProcessingStartedAt(now);
        }

        return events;
    }

    @Transactional
    public void recoverStaleEvents() {

        Instant cutoff =
                Instant.now()
                        .minusSeconds(120);

        List<OutboxEvent> staleEvents =
                outboxEventRepository
                        .findStaleProcessingEvents(
                                OutboxStatus.PROCESSING,
                                cutoff
                        );

        for (OutboxEvent event : staleEvents) {

            event.setStatus(
                    OutboxStatus.PENDING
            );

            event.setProcessingStartedAt(null);
        }
    }
}
