WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY amount NULLS FIRST) AS row_num
        FROM orders
    ) x
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT_IF(channel = 'store') AS store_events,
    COUNT(*) AS cnt,
    AVG(discount) AS avg_discount
FROM ranked
GROUP BY 1, 2
