SELECT id, brand, category, stock_qty, MAX(stock_qty) OVER (PARTITION BY brand, category) AS brand_max_stock_qty FROM products;
