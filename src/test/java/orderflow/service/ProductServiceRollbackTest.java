package orderflow.service;

import orderflow.model.Product;
import orderflow.repository.OutboxEventRepository;
import orderflow.repository.ProductRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@SpringBootTest
@ActiveProfiles("test")
class ProductServiceRollbackTest {

    @Autowired
    private ProductService productService;

    @Autowired
    private ProductRepository productRepository;

    @MockitoBean
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
    void shouldRollbackInventoryWhenOutboxSaveFails() {

        doThrow(new RuntimeException("Simulated outbox failure"))
                .when(outboxEventRepository)
                .save(any());

        assertThrows(
                RuntimeException.class,
                () -> productService.purchaseProduct(productId)
        );

        Product product =
                productRepository.findById(productId)
                        .orElseThrow();

        assertEquals(
                1,
                product.getQuantity()
        );
    }
}