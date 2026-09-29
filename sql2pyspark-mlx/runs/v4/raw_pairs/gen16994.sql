SELECT supplier_id, category FROM products EXCEPT ALL SELECT supplier_id, category FROM products WHERE stock_qty >= 110
