SELECT stock_qty, ARRAY_CONTAINS(tags, 'premium') AS is_premium, cost + price AS margin FROM products WHERE product_name LIKE 'B%' OR stock_qty >= 128;
