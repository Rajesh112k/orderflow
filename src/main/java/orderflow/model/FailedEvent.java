package orderflow.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;


@Entity

/*
 * This maps the Java class:
 *
 *     FailedEvent
 *
 * to the PostgreSQL table:
 *
 *     failed_events
 */
@Table(
        name = "failed_events",

        /*
         * =========================================================
         * UNIQUE CONSTRAINT
         * =========================================================
         *
         * Kafka's physical record identity is:
         *
         *     topic + partition + offset
         *
         * Example:
         *
         *     order-events
         *     partition 2
         *     offset 45
         *
         * That combination uniquely identifies one Kafka record.
         *
         * We use the ORIGINAL topic/partition/offset from the
         * failed record.
         *
         * This prevents the same failed Kafka record from being
         * persisted twice.
         */
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_failed_event_original_record",
                        columnNames = {
                                "original_topic",
                                "original_partition",
                                "original_offset"
                        }
                )
        },

        /*
         * =========================================================
         * DATABASE INDEXES
         * =========================================================
         *
         * These make common searches faster.
         *
         * Example:
         *
         * WHERE event_id = ?
         *
         * or:
         *
         * WHERE status = 'FAILED'
         */
        indexes = {
                @Index(
                        name = "idx_failed_events_event_id",
                        columnList = "event_id"
                ),

                @Index(
                        name = "idx_failed_events_status",
                        columnList = "status"
                )
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FailedEvent {


    /*
     * ============================================================
     * DATABASE PRIMARY KEY
     * ============================================================
     *
     * This identifies the row inside PostgreSQL.
     *
     * Example:
     *
     * id = 1
     * id = 2
     * id = 3
     *
     * This is NOT the Kafka eventId.
     */
    @Id
    @GeneratedValue(
            strategy = GenerationType.IDENTITY
    )
    private Long id;


    /*
     * ============================================================
     * LOGICAL EVENT ID
     * ============================================================
     *
     * Example:
     *
     * eventId = "evt-123"
     *
     * This represents the logical business event.
     */
    @Column(
            name = "event_id",
            nullable = false
    )
    private String eventId;


    /*
     * ============================================================
     * ORIGINAL KAFKA TOPIC
     * ============================================================
     *
     * The event might currently be in:
     *
     *     order-events.DLT
     *
     * but it originally failed in:
     *
     *     order-events
     */
    @Column(
            name = "original_topic",
            nullable = false
    )
    private String originalTopic;


    /*
     * ============================================================
     * ORIGINAL KAFKA PARTITION
     * ============================================================
     *
     * Example:
     *
     * partition = 2
     */
    @Column(
            name = "original_partition",
            nullable = false
    )
    private Integer originalPartition;


    /*
     * ============================================================
     * ORIGINAL KAFKA OFFSET
     * ============================================================
     *
     * Example:
     *
     * offset = 45
     */
    @Column(
            name = "original_offset",
            nullable = false
    )
    private Long originalOffset;


    /*
     * ============================================================
     * ORIGINAL KAFKA KEY
     * ============================================================
     *
     * Example:
     *
     * "777"
     *
     * We keep this because replaying with the same key helps
     * preserve the intended partitioning behavior.
     */
    @Column(
            name = "original_key"
    )
    private String originalKey;


    /*
     * ============================================================
     * ORIGINAL JSON PAYLOAD
     * ============================================================
     *
     * Example:
     *
     * {
     *   "eventId": "evt-123",
     *   "eventType": "PRODUCT_PURCHASED",
     *   "productId": 777,
     *   "quantity": 1
     * }
     *
     * TEXT is used because event JSON may be larger than a normal
     * varchar.
     */
    @Column(
            name = "payload",
            nullable = false,
            columnDefinition = "TEXT"
    )
    private String payload;


    /*
     * ============================================================
     * EXCEPTION CLASS
     * ============================================================
     *
     * Example:
     *
     * java.lang.RuntimeException
     */
    @Column(
            name = "exception_class"
    )
    private String exceptionClass;


    /*
     * ============================================================
     * EXCEPTION MESSAGE
     * ============================================================
     *
     * Example:
     *
     * "Simulated temporary processing failure"
     */
    @Column(
            name = "exception_message",
            columnDefinition = "TEXT"
    )
    private String exceptionMessage;


    /*
     * ============================================================
     * FAILED EVENT STATUS
     * ============================================================
     *
     * Possible values:
     *
     * FAILED
     * REPLAYED
     * RESOLVED
     *
     * EnumType.STRING means PostgreSQL stores:
     *
     *     FAILED
     *
     * rather than:
     *
     *     0
     *
     * which is much safer if enum ordering changes later.
     */
    @Enumerated(EnumType.STRING)
    @Column(
            name = "status",
            nullable = false
    )
    private FailedEventStatus status;


    /*
     * ============================================================
     * REPLAY COUNT
     * ============================================================
     *
     * Starts at:
     *
     * 0
     *
     * Every manual replay can increment it.
     */
    @Column(
            name = "replay_count",
            nullable = false
    )
    private Integer replayCount;


    /*
     * ============================================================
     * CREATED TIME
     * ============================================================
     *
     * When this failed event was first stored.
     */
    @Column(
            name = "created_at",
            nullable = false
    )
    private Instant createdAt;


    /*
     * ============================================================
     * LAST REPLAY TIME
     * ============================================================
     *
     * Initially:
     *
     * null
     *
     * After replay:
     *
     * timestamp of most recent replay attempt.
     */
    @Column(
            name = "last_replayed_at"
    )
    private Instant lastReplayedAt;
    @Column(name = "replay_started_at")
    private Instant replayStartedAt;
}