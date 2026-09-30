SELECT id, brand, price, MIN(price) OVER (PARTITION BY brand) brand_min_price FROM products
