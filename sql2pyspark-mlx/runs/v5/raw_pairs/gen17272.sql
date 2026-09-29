SELECT id, cost, UPPER(category) AS category_upper, LENGTH(brand) AS brand_length FROM products WHERE stock_qty <= 324 ORDER BY brand_length ASC;
