WITH with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        x.device
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.device
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.monthly_fee
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.customer_id = 40
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok
    FROM not_flagged
    WHERE device IS NOT NULL
      AND city IN ('Tokyo', 'Singapore', 'New York')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_bangkok >= 69 THEN 'HIGH' ELSE 'LOW' END AS is_bangkok_tier,
    device,
    city,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS monthly_fee_per_tokyo,
    APPROX_PERCENTILE(is_bangkok, ARRAY(0.25, 0.5, 0.75)) AS is_bangkok_quartiles,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM filtered
GROUP BY is_bangkok_tier, device, city
ORDER BY num_cities NULLS FIRST
