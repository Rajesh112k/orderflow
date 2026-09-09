package orderflow.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import orderflow.dto.CreateProductRequest;
import orderflow.dto.ProductResponse;
import orderflow.service.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    @PostMapping
    public ResponseEntity<ProductResponse> createProduct(
            @Valid @RequestBody CreateProductRequest request) {

        ProductResponse product =
                productService.createProduct(request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(product);
    }

    @PostMapping("/{id}/purchase")
    public ProductResponse purchaseProduct(
            @PathVariable Long id) {

        return productService.purchaseProduct(id);
    }

    @GetMapping
    public List<ProductResponse> getAllProducts() {

        return productService.getAllProducts();
    }

    @GetMapping("/{id}")
    public ProductResponse getProductById(
            @PathVariable Long id) {

        return productService.getProductById(id);
    }
}