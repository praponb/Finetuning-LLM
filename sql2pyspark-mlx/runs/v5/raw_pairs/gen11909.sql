WITH kept_keys AS (
    SELECT status, method
    FROM payments
    INTERSECT ALL
    SELECT status, method
    FROM payments
    WHERE order_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM payments s
    JOIN kept_keys k ON s.status = k.status AND s.method = k.method
)
SELECT
    CASE WHEN amount >= 1610 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    COUNT(*) AS cnt,
    MIN(paid_at) AS started_at,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS amount_per_success,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN method = 'card' THEN 1 END), 0), 2) AS amount_per_card,
    GROUPING_ID() AS gid
FROM kept_rows
GROUP BY ROLLUP (CASE WHEN amount >= 1610 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', paid_at))
ORDER BY cnt NULLS FIRST
