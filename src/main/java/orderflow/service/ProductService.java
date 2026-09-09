package orderflow.service;


import lombok.RequiredArgsConstructor;
import orderflow.dto.CreateProductRequest;
import orderflow.dto.ProductResponse;
import orderflow.event.OrderEvent;
import orderflow.exception.OutOfStockException;
import orderflow.exception.ProductNotFoundException;
import orderflow.model.OutboxEvent;
import orderflow.model.OutboxStatus;
import orderflow.model.Product;
import orderflow.producer.OrderEventProducer;
import orderflow.repository.OutboxEventRepository;
import orderflow.repository.ProductRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final OutboxEventRepository outboxEventRepository;

    public ProductResponse createProduct(CreateProductRequest request) {

        Product product = Product.builder()
                .name(request.getName())
                .price(request.getPrice())
                .quantity(request.getQuantity())
                .build();

        Product savedProduct = productRepository.save(product);

        return mapToResponse(savedProduct);
    }

    public List<ProductResponse> getAllProducts() {

        return productRepository.findAll()
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    private ProductResponse mapToResponse(Product product) {

        return ProductResponse.builder()
                .id(product.getId())
                .name(product.getName())
                .price(product.getPrice())
                .quantity(product.getQuantity())
                .build();
    }

    public ProductResponse getProductById(Long id) {

        Product product = productRepository.findById(id)
                .orElseThrow(() ->
                        new ProductNotFoundException(id));

        return mapToResponse(product);
    }
    @Transactional
    public ProductResponse purchaseProduct(Long productId) {

        int updatedRows =
                productRepository.decrementQuantityIfAvailable(productId);

        if (updatedRows == 0) {

            if (!productRepository.existsById(productId)) {
                throw new ProductNotFoundException(productId);
            }

            throw new OutOfStockException(productId);
        }

        String eventId = UUID.randomUUID().toString();

        String payload = """
            {
              "eventId": "%s",
              "eventType": "PRODUCT_PURCHASED",
              "productId": %d,
              "quantity": 1,
              "occurredAt": "%s"
            }
            """.formatted(
                eventId,
                productId,
                Instant.now()
        );

        Instant now = Instant.now();
        OutboxEvent outboxEvent =
                OutboxEvent.builder()
                        .id(eventId)
                        .aggregateType("PRODUCT")
                        .aggregateId(productId)
                        .eventType("PRODUCT_PURCHASED")
                        .payload(payload)
                        .createdAt(now)
                        .status(OutboxStatus.PENDING)
                        .retryCount(0)
                        .lastAttemptAt(null)
                        .nextAttemptAt(now)
                        .processingStartedAt(null)
                        .build();
        outboxEventRepository.save(outboxEvent);

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        return mapToResponse(product);
    }
}