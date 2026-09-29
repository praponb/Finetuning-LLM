SELECT
    id,
    product_name,
    brand,
    price,
    stock_qty,
    MIN(price) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_price,
    AVG(price) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_price,
    MAX(price) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_price,
    COUNT(price) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_price
FROM products
WHERE supplier_id IS NOT NULL;
