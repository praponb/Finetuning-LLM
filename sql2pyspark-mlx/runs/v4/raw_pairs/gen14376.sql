WITH with_last AS (
    SELECT
        s.*,
        latest.unit_price,
        latest.quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    category,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    COUNT(*) FILTER (WHERE category = 'Home') AS home_rows,
    COUNT(*) AS event_count,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) AS event_count_dense_rank
FROM with_last
GROUP BY 1, 2
HAVING event_count > 1
ORDER BY event_count
