SELECT id, price, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE stock_qty >= 198
