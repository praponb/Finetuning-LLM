WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS tag
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(tag) AS tag_lower,
        CASE WHEN tag IN ('SPRING10', 'VIP') THEN 'Priority' ELSE 'Other' END AS tag_group,
        COALESCE(tag, 'N/A') AS tag_filled
    FROM flattened
    WHERE created_at > DATE '2025-03-02'
      AND customer_id = 38
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.credit_limit,
        d.city,
        d.segment
    FROM valid src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.country,
        p.age,
        p.segment AS customer_segment
    FROM joined a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(age / credit_limit, 2) AS age_per_credit_limit
    FROM joined_carts
    WHERE created_at > TIMESTAMP '2025-03-01 15:00:00'
      AND city <> 'Tokyo'
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    created_at,
    tag,
    tag_lower,
    tag_group,
    tag_filled,
    credit_limit,
    city,
    segment,
    country,
    age,
    customer_segment,
    age_per_credit_limit,
    RANK() OVER (PARTITION BY customer_segment ORDER BY age_per_credit_limit DESC NULLS LAST) AS age_per_credit_limit_rank,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id), 4) AS credit_limit_share,
    MIN(credit_limit) OVER w_hist AS customer_segment_min_credit_limit,
    CUME_DIST() OVER w_hist_2 AS credit_limit_cume_dist
FROM filtered
WINDOW w_hist AS (PARTITION BY customer_segment), w_hist_2 AS (ORDER BY credit_limit DESC);
