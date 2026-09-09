package orderflow.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;

import org.springframework.util.backoff.FixedBackOff;

import tools.jackson.core.JacksonException;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class KafkaConsumerConfig {


    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;


    /*
     * ============================================================
     * COMMON CONSUMER FACTORY
     * ============================================================
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        Map<String, Object> properties =
                new HashMap<>();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        return new DefaultKafkaConsumerFactory<>(
                properties
        );
    }


    /*
     * ============================================================
     * MAIN BUSINESS CONSUMER DLT RECOVERER
     * ============================================================
     *
     * Failed records from:
     *
     * order-events
     *
     * are published to:
     *
     * order-events.DLT
     */
    @Bean
    public DeadLetterPublishingRecoverer
    deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> kafkaTemplate
    ) {

        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,

                (record, exception) ->
                        new TopicPartition(
                                record.topic() + ".DLT",
                                record.partition()
                        )
        );
    }


    /*
     * ============================================================
     * MAIN BUSINESS CONSUMER ERROR HANDLER
     * ============================================================
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer recoverer
    ) {

        /*
         * Initial attempt
         * +
         * 2 retries
         *
         * = 3 total attempts
         */
        FixedBackOff fixedBackOff =
                new FixedBackOff(
                        2000L,
                        2L
                );

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(
                        recoverer,
                        fixedBackOff
                );

        /*
         * Malformed JSON is not transient.
         *
         * Retrying it will not fix the JSON.
         */
        errorHandler.addNotRetryableExceptions(
                JacksonException.class
        );

        return errorHandler;
    }


    /*
     * ============================================================
     * MAIN ORDER EVENT LISTENER FACTORY
     * ============================================================
     */
    @Bean(
            name =
                    "orderEventKafkaListenerContainerFactory"
    )
    public ConcurrentKafkaListenerContainerFactory
            <String, String>
    orderEventKafkaListenerContainerFactory(

            ConsumerFactory<String, String>
                    consumerFactory,

            DefaultErrorHandler
                    kafkaErrorHandler
    ) {

        ConcurrentKafkaListenerContainerFactory
                <String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                consumerFactory
        );

        factory.setCommonErrorHandler(
                kafkaErrorHandler
        );

        /*
         * order-events has three partitions.
         */
        factory.setConcurrency(
                3
        );

        return factory;
    }


    /*
     * ============================================================
     * DLT STORAGE ERROR HANDLER
     * ============================================================
     *
     * IMPORTANT:
     *
     * There is NO DeadLetterPublishingRecoverer here.
     *
     * Therefore:
     *
     * order-events.DLT
     *      ↓
     * PostgreSQL failure
     *      ↓
     * retry
     *
     * but NOT:
     *
     * order-events.DLT.DLT
     */
    @Bean
    public DefaultErrorHandler
    dltStorageErrorHandler() {

        /*
         * Wait 5 seconds between attempts.
         *
         * Retry 2 additional times.
         *
         * Initial
         * +
         * retry #1
         * +
         * retry #2
         *
         * = 3 attempts
         */
        FixedBackOff fixedBackOff =
                new FixedBackOff(
                        5000L,
                        2L
                );


        return new DefaultErrorHandler(
                fixedBackOff
        );
    }


    /*
     * ============================================================
     * DLT STORAGE LISTENER FACTORY
     * ============================================================
     */
    @Bean(
            name =
                    "dltStorageKafkaListenerContainerFactory"
    )
    public ConcurrentKafkaListenerContainerFactory
            <String, String>
    dltStorageKafkaListenerContainerFactory(

            ConsumerFactory<String, String>
                    consumerFactory,

            DefaultErrorHandler
                    dltStorageErrorHandler
    ) {

        ConcurrentKafkaListenerContainerFactory
                <String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();


        factory.setConsumerFactory(
                consumerFactory
        );


        /*
         * Notice:
         *
         * This uses dltStorageErrorHandler,
         *
         * NOT kafkaErrorHandler.
         */
        factory.setCommonErrorHandler(
                dltStorageErrorHandler
        );


        /*
         * DLT also has three partitions.
         */
        factory.setConcurrency(
                3
        );


        return factory;
    }
}