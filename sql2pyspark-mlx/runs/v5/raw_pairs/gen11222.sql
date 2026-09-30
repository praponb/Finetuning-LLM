WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.credit_limit,
        p.segment
    FROM exploded e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value,
        segment,
        CASE WHEN credit_limit >= 14000 THEN 'High' WHEN credit_limit >= 5200 THEN 'Medium' ELSE 'Low' END AS credit_limit_band,
        CAST(credit_limit AS INT) AS credit_limit_int
    FROM joined
    WHERE attr_key IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at,
        e.value,
        e.segment,
        e.credit_limit_int,
        p.email,
        p.credit_limit
    FROM filtered e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN credit_limit >= 14800 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
        MAX(credit_limit) AS max_credit_limit,
        SUM(CASE WHEN segment = 'Corporate' THEN credit_limit ELSE 0 END) AS corporate_credit_limit,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        COUNT(*) AS row_count,
        COUNT(DISTINCT segment) AS distinct_segments,
        MAX(credit_limit) AS max_credit_limit_2
    FROM enriched
    GROUP BY 1
    HAVING COUNT(DISTINCT segment) >= 1
)
SELECT
    credit_limit_tier,
    max_credit_limit,
    corporate_credit_limit,
    row_count,
    distinct_segments,
    max_credit_limit_2
FROM stats
ORDER BY max_credit_limit
