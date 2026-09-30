WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.cost < 41
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.method = 'card'
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.name,
        c.segment
    FROM eligible a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAYOFWEEK(order_date) AS order_date_dayofweek
    FROM enriched
    WHERE order_date >= DATE '2023-12-02'
      AND channel IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount >= 0.36 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    COUNT_IF(segment = 'SMB') AS smb_events,
    COUNT(*) AS event_count,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS amount_per_mobile
FROM base
GROUP BY CASE WHEN discount >= 0.36 THEN 'HIGH' ELSE 'LOW' END
ORDER BY amount_per_mobile;
