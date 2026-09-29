WITH filtered_order_items AS (
    SELECT *
    FROM order_items
    WHERE order_id IN (6, 21, 24, 31)
),
order_id_summary AS (
    SELECT
        order_id,
        CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        AVG(quantity) AS avg_quantity,
        MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM filtered_order_items
    GROUP BY order_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM order_id_summary
WHERE avg_quantity < 4.1
ORDER BY order_id ASC;
