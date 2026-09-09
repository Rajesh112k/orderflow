package orderflow.repository;

import orderflow.model.Product;

import orderflow.service.PurchaseTransactionService;
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
class ProductRepositoryConcurrencyTest {

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private PurchaseTransactionService purchaseTransactionService;

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

        productId = savedProduct.getId();
    }


    @Test
    void shouldDecrementInventoryWhenProductIsAvailable() {

        int updatedRows =
                purchaseTransactionService
                        .attemptPurchase(productId);

        Product product =
                productRepository.findById(productId)
                        .orElseThrow();

        assertEquals(1, updatedRows);
        assertEquals(0, product.getQuantity());
    }


    @Test
    void shouldAllowOnlyOneSuccessfulPurchaseWhenInventoryIsOne()
            throws Exception {

        int numberOfThreads = 10;

        ExecutorService executorService =
                Executors.newFixedThreadPool(numberOfThreads);

        CountDownLatch readyLatch =
                new CountDownLatch(numberOfThreads);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        List<Future<Integer>> futures =
                new ArrayList<>();

        for (int i = 0; i < numberOfThreads; i++) {

            Future<Integer> future =
                    executorService.submit(() -> {

                        readyLatch.countDown();

                        startLatch.await();

                        return purchaseTransactionService
                                .attemptPurchase(productId);
                    });

            futures.add(future);
        }

        readyLatch.await();

        startLatch.countDown();

        int successfulPurchases = 0;
        int failedPurchases = 0;

        for (Future<Integer> future : futures) {

            int updatedRows = future.get();

            if (updatedRows == 1) {
                successfulPurchases++;
            } else if (updatedRows == 0) {
                failedPurchases++;
            }
        }

        Product product =
                productRepository.findById(productId)
                        .orElseThrow();

        assertEquals(1, successfulPurchases);
        assertEquals(9, failedPurchases);
        assertEquals(0, product.getQuantity());

        executorService.shutdown();
    }
}