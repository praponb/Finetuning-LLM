WITH kept_keys AS (
    SELECT browser
    FROM sessions
    EXCEPT ALL
    SELECT browser
    FROM sessions
    WHERE TO_DATE(started_at) >= '2024-01-01'
),
kept_rows AS (
    SELECT s.*
    FROM sessions s
    JOIN kept_keys k ON s.browser = k.browser
)
SELECT
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    (SELECT AVG(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_discount
FROM kept_rows s
ORDER BY browser DESC
