WITH base AS (
    SELECT *
    FROM shipments
    WHERE order_id IS NOT NULL
),
summary AS (
    SELECT status, MAX(shipped_date) AS last_shipped_date, ROUND((100 * SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_returned
    FROM base
    GROUP BY status
)
SELECT *
FROM summary
