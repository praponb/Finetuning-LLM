SELECT
    id,
    brand,
    stock_qty,
    MAX(stock_qty) OVER (PARTITION BY brand) AS brand_max_stock_qty,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty, id) AS rn
FROM products
WHERE product_name LIKE 'W%';
