WITH src AS (
    SELECT *
    FROM orders
    WHERE order_date <= '2025-10-11'
),
status_summary AS (
    SELECT status, MIN(order_date) AS min_order_date, COUNT(*) AS total_count
    FROM src
    GROUP BY status
)
SELECT status, min_order_date, total_count
FROM status_summary
ORDER BY min_order_date DESC, status
LIMIT 20;
