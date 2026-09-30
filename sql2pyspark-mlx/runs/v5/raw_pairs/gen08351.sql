WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(stock_qty) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty,
        AVG(stock_qty) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_stock_qty,
        DENSE_RANK() OVER (PARTITION BY category, brand ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_dense_rank
    FROM products
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    (SELECT MAX(t2.price) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS max_product_price
FROM scored s
ORDER BY brand DESC NULLS LAST;
