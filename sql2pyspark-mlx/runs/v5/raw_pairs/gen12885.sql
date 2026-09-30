SELECT
    id,
    product_id,
    unit_price,
    MIN(quantity) OVER w_part AS running_min_quantity,
    AVG(quantity) OVER w_part AS running_avg_quantity,
    COUNT(quantity) OVER w_part AS running_count_quantity,
    NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket
FROM order_items
WINDOW w_part AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
