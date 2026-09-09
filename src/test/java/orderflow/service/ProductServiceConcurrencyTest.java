package orderflow.service;

import orderflow.exception.OutOfStockException;
import orderflow.model.Product;
import orderflow.repository.OutboxEventRepository;
import orderflow.repository.ProcessedEventRepository;
import orderflow.repository.ProductRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
class ProductServiceConcurrencyTest {

    @Autowired
    private ProductService productService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    private Long productId;

    @BeforeEach
    void setUp() {

        Product product =
                Product.builder()
                        .name("PlayStation 5")
                        .price(new BigDecimal("499.99"))
                        .quantity(1)
                        .build();

        Product savedProduct =
                productRepository.save(product);

        productId =
                savedProduct.getId();
    }

    @Test
    void shouldAllowOnlyOneSuccessfulPurchaseAndCreateOneOutboxEvent()
            throws Exception {

        int numberOfThreads = 10;

        ExecutorService executorService =
                Executors.newFixedThreadPool(numberOfThreads);

        CountDownLatch readyLatch =
                new CountDownLatch(numberOfThreads);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        List<Future<Boolean>> futures =
                new ArrayList<>();


        for (int i = 0; i < numberOfThreads; i++) {

            Future<Boolean> future =
                    executorService.submit(() -> {

                        readyLatch.countDown();

                        startLatch.await();

                        try {

                            productService
                                    .purchaseProduct(productId);

                            return true;

                        } catch (OutOfStockException exception) {

                            return false;
                        }
                    });

            futures.add(future);
        }


        readyLatch.await();

        startLatch.countDown();


        int successfulPurchases = 0;
        int failedPurchases = 0;

        for (Future<Boolean> future : futures) {

            boolean success =
                    future.get();

            if (success) {

                successfulPurchases++;

            } else {

                failedPurchases++;
            }
        }


        Product product =
                productRepository.findById(productId)
                        .orElseThrow();

        long outboxEventCount =
                outboxEventRepository
                        .countByAggregateIdAndEventType(
                                productId,
                                "PRODUCT_PURCHASED"
                        );


        assertEquals(
                1,
                successfulPurchases
        );

        assertEquals(
                9,
                failedPurchases
        );

        assertEquals(
                0,
                product.getQuantity()
        );

        assertEquals(
                1,
                outboxEventCount
        );


        executorService.shutdown();
    }

    @SpringBootTest
    @ActiveProfiles("test")
    static
    class OrderEventProcessingServiceIntegrationTest {

        @Autowired
        private OrderEventProcessingService processingService;

        @Autowired
        private ProcessedEventRepository processedEventRepository;

    }
}