SELECT COUNT(CASE WHEN brand = 'Stark' THEN 1 END) stark_count FROM products WHERE stock_qty > 268 AND price > 470;
