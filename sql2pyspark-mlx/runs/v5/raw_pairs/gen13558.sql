WITH valid_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity <= 7
),
order_items_stats AS (
    SELECT
        order_id,
        CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END unit_price_bucket,
        COUNT(*) total_count,
        COUNT(*) row_count
    FROM valid_order_items
    GROUP BY order_id, CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END
)
SELECT
    order_id,
    unit_price_bucket,
    total_count,
    row_count
FROM order_items_stats
WHERE row_count > 5
