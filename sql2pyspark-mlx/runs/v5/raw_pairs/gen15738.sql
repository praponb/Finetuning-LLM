SELECT id, product_name, brand, EXPLODE(tags) AS label FROM products WHERE stock_qty >= 221;
