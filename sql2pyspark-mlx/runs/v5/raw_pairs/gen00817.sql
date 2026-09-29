WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS value
),
base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM flattened
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 11:00:00')
      AND value IS NOT NULL
),
base_carts AS (
    SELECT
        id,
        customer_id,
        created_at,
        value,
        days_since_created_at,
        CEIL(days_since_created_at) AS days_since_created_at_whole,
        CASE WHEN days_since_created_at >= 62 THEN 'High' WHEN days_since_created_at >= 26 THEN 'Medium' ELSE 'Low' END AS days_since_created_at_level
    FROM base
    WHERE created_at >= DATE '2025-03-02'
),
joined AS (
    SELECT
        s.*,
        d.name,
        d.segment
    FROM base_carts s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
joined_carts AS (
    SELECT
        s.customer_id,
        s.created_at,
        s.days_since_created_at,
        p.segment AS customer_segment,
        p.signup_date
    FROM joined s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
)
SELECT
    CASE WHEN days_since_created_at >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_created_at_tier,
    COUNT_IF(customer_segment = 'Corporate') AS corporate_events,
    PERCENTILE_APPROX(days_since_created_at, 0.9) AS days_since_created_at_p90,
    SUM(CASE WHEN customer_segment = 'SMB' THEN days_since_created_at ELSE 0 END) AS smb_days_since_created_at,
    MAX(created_at) AS last_seen,
    ROUND(SUM(days_since_created_at) / NULLIF(COUNT(CASE WHEN customer_segment = 'SMB' THEN 1 END), 0), 2) AS days_since_created_at_per_smb,
    ROUND(SUM(days_since_created_at) / NULLIF(COUNT(CASE WHEN customer_segment = 'Corporate' THEN 1 END), 0), 2) AS days_since_created_at_per_corporate,
    GROUPING_ID() AS grouping_id
FROM joined_carts
GROUP BY ROLLUP (CASE WHEN days_since_created_at >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY days_since_created_at_per_corporate NULLS LAST;
