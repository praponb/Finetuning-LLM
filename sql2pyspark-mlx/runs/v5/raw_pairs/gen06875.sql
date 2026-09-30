SELECT id, price, brand, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE stock_qty >= 199
