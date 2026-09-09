package orderflow.service;

import lombok.RequiredArgsConstructor;

import orderflow.repository.FailedEventRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;


@Service
@RequiredArgsConstructor
public class FailedEventReplayStateService {

    private final FailedEventRepository failedEventRepository;
    private static final int MAX_REPLAYS = 3;


    /*
     * Try to transition:
     *
     * FAILED
     *   ↓
     * REPLAYING
     *
     * Returns:
     *
     * true  = claim successful
     * false = someone else already claimed it
     */
    @Transactional
    public boolean claim(Long failedEventId) {

        int updated =
                failedEventRepository.claimForReplay(
                        failedEventId,
                        Instant.now(),
                        MAX_REPLAYS
                );

        return updated == 1;
    }

    /*
     * REPLAYING
     *    ↓
     * REPLAYED
     */
    @Transactional
    public void markSucceeded(
            Long failedEventId
    ) {

        int updated =
                failedEventRepository
                        .markReplaySucceeded(
                                failedEventId,
                                Instant.now()
                        );


        if (updated != 1) {

            throw new IllegalStateException(
                    "Could not mark failed event as REPLAYED: "
                            + failedEventId
            );
        }
    }


    /*
     * Kafka publication failed.
     *
     * REPLAYING
     *    ↓
     * FAILED
     *
     * This makes the event eligible for another
     * manual replay later.
     */
    @Transactional
    public void releaseClaim(
            Long failedEventId
    ) {

        int updated =
                failedEventRepository
                        .releaseReplayClaim(
                                failedEventId
                        );


        if (updated != 1) {

            throw new IllegalStateException(
                    "Could not release replay claim for failed event: "
                            + failedEventId
            );
        }
    }
    @Transactional
    public int recoverStaleReplays() {
        return failedEventRepository.recoverStaleReplays();
    }
}