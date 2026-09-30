WITH kept AS (
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IS NULL
        )
)
SELECT
    category,
    SUM(price) FILTER (WHERE brand = 'Acme') AS acme_total_price,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev,
    SUM(stock_qty) FILTER (WHERE category = 'Books') AS books_total_stock_qty
FROM kept
GROUP BY ROLLUP (category)
