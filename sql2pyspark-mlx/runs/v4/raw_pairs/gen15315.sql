SELECT id, category FROM products WHERE category IN ('Books', 'Fashion', 'Electronics') EXCEPT SELECT id, category FROM products WHERE stock_qty <= 344
