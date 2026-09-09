package orderflow.repository;

import orderflow.model.FailedEvent;
import orderflow.model.FailedEventStatus;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;


public interface FailedEventRepository
        extends JpaRepository<FailedEvent, Long> {


    /*
     * Find all failed events in a particular administrative state.
     *
     * Example:
     *
     * FAILED
     * REPLAYED
     * RESOLVED
     */
    List<FailedEvent> findByStatus(
            FailedEventStatus status
    );


    /*
     * Find failures for one logical business event.
     *
     * Example:
     *
     * eventId = evt-123
     */
    List<FailedEvent> findByEventId(
            String eventId
    );


    /*
     * ============================================================
     * IDEMPOTENT DLT INSERT
     * ============================================================
     *
     * We identify the original physical Kafka record using:
     *
     * original_topic
     * +
     * original_partition
     * +
     * original_offset
     *
     * The database already has a UNIQUE constraint over these
     * three columns.
     *
     * Therefore PostgreSQL can safely do:
     *
     * INSERT ...
     * ON CONFLICT (...) DO NOTHING
     *
     *
     * Return value:
     *
     * 1 = a new row was inserted
     *
     * 0 = this exact Kafka failure was already stored
     */
    @Modifying
    @Query(
            value = """
                    INSERT INTO failed_events (
                        event_id,
                        original_topic,
                        original_partition,
                        original_offset,
                        original_key,
                        payload,
                        exception_class,
                        exception_message,
                        status,
                        replay_count,
                        created_at,
                        last_replayed_at
                    )
                    VALUES (
                        :eventId,
                        :originalTopic,
                        :originalPartition,
                        :originalOffset,
                        :originalKey,
                        :payload,
                        :exceptionClass,
                        :exceptionMessage,
                        :status,
                        :replayCount,
                        :createdAt,
                        :lastReplayedAt
                    )
                    ON CONFLICT (
                        original_topic,
                        original_partition,
                        original_offset
                    )
                    DO NOTHING
                    """,
            nativeQuery = true
    )
    int insertIfNotExists(
            @Param("eventId")
            String eventId,

            @Param("originalTopic")
            String originalTopic,

            @Param("originalPartition")
            Integer originalPartition,

            @Param("originalOffset")
            Long originalOffset,

            @Param("originalKey")
            String originalKey,

            @Param("payload")
            String payload,

            @Param("exceptionClass")
            String exceptionClass,

            @Param("exceptionMessage")
            String exceptionMessage,

            @Param("status")
            String status,

            @Param("replayCount")
            Integer replayCount,

            @Param("createdAt")
            Instant createdAt,

            @Param("lastReplayedAt")
            Instant lastReplayedAt
    );

    @Modifying
    @Query(
            value = """
                UPDATE failed_events
                SET status = 'REPLAYING',
                    replay_started_at = :startedAt
                WHERE id = :id
                  AND status = 'FAILED'
                  AND replay_count < :maxReplays
                """,
            nativeQuery = true
    )
    int claimForReplay(
            @Param("id") Long id,
            @Param("startedAt") Instant startedAt,
            @Param("maxReplays") Integer maxReplays
    );

    @Modifying
    @Query(
            value = """
                UPDATE failed_events
                SET status = 'REPLAYED',
                    replay_count = replay_count + 1,
                    last_replayed_at = :replayedAt,
                    replay_started_at = NULL
                WHERE id = :id
                  AND status = 'REPLAYING'
                """,
            nativeQuery = true
    )
    int markReplaySucceeded(
            @Param("id") Long id,
            @Param("replayedAt") Instant replayedAt
    );

    @Modifying
    @Query(
            value = """
                UPDATE failed_events
                SET status = 'FAILED',
                    replay_started_at = NULL
                WHERE id = :id
                  AND status = 'REPLAYING'
                """,
            nativeQuery = true
    )
    int releaseReplayClaim(
            @Param("id") Long id
    );
    @Modifying
    @Query(
            value = """
                UPDATE failed_events
                SET status = 'FAILED',
                    replay_started_at = NULL
                WHERE status = 'REPLAYING'
                  AND replay_started_at <
                      CURRENT_TIMESTAMP - INTERVAL '2 minutes'
                """,
            nativeQuery = true
    )
    int recoverStaleReplays();
}