package orderflow.model;

public enum OutboxStatus {
    PENDING,
    PUBLISHED,
    FAILED,
    PROCESSING
}