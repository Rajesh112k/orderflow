package orderflow.consumer;

import lombok.RequiredArgsConstructor;

import orderflow.event.OrderEvent;
import orderflow.service.OrderEventProcessingService;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;


@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ObjectMapper objectMapper;

    private final OrderEventProcessingService processingService;


    @KafkaListener(
            topics = "order-events",
            groupId =
                    "${orderflow.kafka.consumer.group-id:orderflow-consumer}",
            containerFactory =
                    "orderEventKafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, String> record
    ) throws Exception {


        System.out.println(
                "\n========== ORDER EVENT RECEIVED =========="
        );


        System.out.println(
                "Topic     : "
                        + record.topic()
        );


        System.out.println(
                "Partition : "
                        + record.partition()
        );


        System.out.println(
                "Offset    : "
                        + record.offset()
        );


        System.out.println(
                "Key       : "
                        + record.key()
        );


        System.out.println(
                "Thread    : "
                        + Thread
                        .currentThread()
                        .getName()
        );


        System.out.println(
                "Payload   : "
                        + record.value()
        );


        System.out.println(
                "==========================================\n"
        );


        /*
         * Deserialize JSON.
         *
         * Malformed JSON causes JacksonException.
         *
         * We configured JacksonException as:
         *
         * non-retryable
         *
         * so it should go directly to DLT.
         */
        OrderEvent event =
                objectMapper.readValue(
                        record.value(),
                        OrderEvent.class
                );


        /*
         * IMPORTANT:
         *
         * Do NOT catch exceptions here.
         *
         * RuntimeException must escape this method
         * so DefaultErrorHandler can see it.
         */
        processingService.process(
                event
        );
    }
}