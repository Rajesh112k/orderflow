package orderflow.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class OrderEventDltConsumer {

    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "orderflow-dlt-consumer"
    )
    public void consumeDlt(
            ConsumerRecord<String, String> record
    ) {

        System.out.println();
        System.out.println("========== DLT MESSAGE ==========");

        System.out.println(
                "DLT Topic: "
                        + record.topic()
        );

        System.out.println(
                "DLT Partition: "
                        + record.partition()
        );

        System.out.println(
                "DLT Offset: "
                        + record.offset()
        );

        System.out.println(
                "Key: "
                        + record.key()
        );

        System.out.println(
                "Value: "
                        + record.value()
        );

        System.out.println("---------- HEADERS ----------");

        for (Header header : record.headers()) {

            String headerValue =
                    new String(
                            header.value(),
                            StandardCharsets.UTF_8
                    );

            System.out.println(
                    header.key()
                            + " = "
                            + headerValue
            );
        }

        System.out.println("==============================");
        System.out.println();
    }
}