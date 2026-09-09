package orderflow.service;

import orderflow.exception.OutOfStockException;
import orderflow.exception.ProductNotFoundException;
import orderflow.model.Product;
import orderflow.repository.OutboxEventRepository;
import orderflow.repository.ProductRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProductServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private ProductService productService;


    @BeforeEach
    void setUp() {

        MockitoAnnotations.openMocks(this);

        productService =
                new ProductService(
                        productRepository,
                        outboxEventRepository
                );
    }
    @Test
    void shouldReturnProductWhenProductExists() {

        Product product =
                Product.builder()
                        .id(1L)
                        .name("PlayStation 5")
                        .price(
                                new BigDecimal("499.99")
                        )
                        .quantity(5)
                        .build();

        when(
                productRepository.findById(1L)
        ).thenReturn(
                Optional.of(product)
        );

        var response =
                productService.getProductById(1L);

        assertEquals(
                1L,
                response.getId()
        );

        assertEquals(
                "PlayStation 5",
                response.getName()
        );

        assertEquals(
                new BigDecimal("499.99"),
                response.getPrice()
        );

        assertEquals(
                5,
                response.getQuantity()
        );
    }

    @Test
    void shouldThrowProductNotFoundExceptionWhenProductDoesNotExist() {

        // Arrange
        when(
                productRepository.findById(999L)
        ).thenReturn(
                Optional.empty()
        );

        // Act + Assert
        ProductNotFoundException exception =
                assertThrows(
                        ProductNotFoundException.class,
                        () -> productService.getProductById(999L)
                );

        // Assert exception details
        assertEquals(
                "Product not found with id: 999",
                exception.getMessage()
        );

        // Verify repository interaction
        verify(productRepository)
                .findById(999L);
    }

    @Test
    void shouldThrowOutOfStockExceptionWhenProductHasNoInventory() {

        Long productId = 1L;

        when(
                productRepository
                        .decrementQuantityIfAvailable(productId)
        ).thenReturn(0);

        when(
                productRepository.existsById(productId)
        ).thenReturn(true);

        assertThrows(
                OutOfStockException.class,
                () -> productService.purchaseProduct(productId)
        );

        verify(productRepository)
                .decrementQuantityIfAvailable(productId);

        verify(productRepository)
                .existsById(productId);

        verifyNoInteractions(outboxEventRepository);
    }

    @Test
    void shouldNotCreateOutboxEventWhenProductIsOutOfStock() {

        Long productId = 1L;

        when(productRepository
                .decrementQuantityIfAvailable(productId))
                .thenReturn(0);

        when(productRepository
                .existsById(productId))
                .thenReturn(true);


        assertThrows(
                OutOfStockException.class,
                () -> productService.purchaseProduct(productId)
        );


        verify(productRepository)
                .decrementQuantityIfAvailable(productId);

        verify(productRepository)
                .existsById(productId);

        verifyNoInteractions(outboxEventRepository);
    }
}