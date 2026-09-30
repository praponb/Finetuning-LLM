WITH eligible AS (
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.delivered_date < s.order_date
        )
)
SELECT
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
FROM eligible
GROUP BY DATE_TRUNC('MONTH', order_date)
