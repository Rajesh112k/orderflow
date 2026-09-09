package orderflow.repository;

import jakarta.persistence.LockModeType;
import orderflow.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface ProductRepository
        extends JpaRepository<Product, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Product p WHERE p.id = :id")
    Optional<Product> findByIdForUpdate(
            @Param("id") Long id
    );
    @Modifying(clearAutomatically = true)
    @Query("""
        UPDATE Product p
        SET p.quantity = p.quantity - 1
        WHERE p.id = :id
        AND p.quantity > 0
        """)
    int decrementQuantityIfAvailable(
            @Param("id") Long id
    );
}
