WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        v,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') is_welcome5,
        COALESCE(k, 'DIRECT') resolved_k
    FROM lines
    WHERE created_at > TIMESTAMP '2025-03-01 19:00:00'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.email,
        b.segment,
        b.name
    FROM filtered e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.v,
        a.is_welcome5,
        a.resolved_k,
        a.segment,
        b.email AS customer_email,
        b.age,
        b.credit_limit
    FROM enriched a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
daily AS (
    -- Grouped metrics
    SELECT
        segment,
        CASE WHEN credit_limit >= 15800 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(age, 0.5) AS age_p50,
        ROUND(AVG(age), 2) AS avg_age,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count
    FROM with_dims
    GROUP BY segment, CASE WHEN credit_limit >= 15800 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING age_p50 < 1000000
)
SELECT
    segment,
    credit_limit_tier,
    avg_age,
    consumer_count,
    CASE WHEN segment RLIKE '^(Consumer|Corporate)$' THEN 'CORE' WHEN segment RLIKE '^(SMB)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS segment_zone
FROM daily
ORDER BY segment_zone DESC;
