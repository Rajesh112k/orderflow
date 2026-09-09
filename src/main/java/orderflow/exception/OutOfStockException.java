package orderflow.exception;

public class OutOfStockException extends RuntimeException {

    public OutOfStockException(Long productId) {
        super("Product is out of stock: " + productId);
    }
}
