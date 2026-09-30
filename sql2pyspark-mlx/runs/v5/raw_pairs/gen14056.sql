WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.element,
        b.email
    FROM lines e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, d.segment
    FROM with_dims s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', segment, email) segment_email_key
    FROM joined
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.segment,
        a.segment_email_key,
        d.city,
        d.age,
        d.name
    FROM valid a
    INNER JOIN customers d
        ON a.customer_id = d.id
    WHERE d.city = 'London'
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT
        segment,
        segment_email_key,
        name,
        PERCENT_RANK() OVER (PARTITION BY segment, customer_id ORDER BY age) age_percent_rank,
        ROUND(age / SUM(age) OVER (PARTITION BY customer_id), 4) age_share
    FROM enriched
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        segment,
        segment_email_key,
        name,
        age_percent_rank,
        age_share,
        LOWER(name) name_lower
    FROM enriched_carts
    WHERE segment IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age_share >= 0.7 THEN 'HIGH' ELSE 'LOW' END AS age_share_tier,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    COUNT_IF(segment = 'SMB') AS smb_events,
    COUNT(DISTINCT segment) AS distinct_segments
FROM valid_carts
GROUP BY CASE WHEN age_share >= 0.7 THEN 'HIGH' ELSE 'LOW' END
ORDER BY age_share_tier DESC NULLS FIRST;
