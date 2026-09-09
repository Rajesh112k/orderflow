package orderflow.service;

import lombok.RequiredArgsConstructor;
import orderflow.repository.ProductRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class PurchaseTransactionService {

    private final ProductRepository productRepository;

    @Transactional
    public int attemptPurchase(Long productId) {

        return productRepository
                .decrementQuantityIfAvailable(productId);
    }
}