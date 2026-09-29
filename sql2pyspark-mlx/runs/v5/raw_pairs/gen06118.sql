WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.email,
        p.age
    FROM exploded a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE p.city IN ('Chiang Mai', 'Berlin', 'Tokyo')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.city,
        c.segment
    FROM joined e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        tag,
        email,
        age,
        segment,
        SUM(age) OVER (PARTITION BY customer_id) AS customer_id_sum_age,
        STDDEV_SAMP(age) OVER (PARTITION BY segment) AS segment_stddev_samp_age,
        DENSE_RANK() OVER (ORDER BY age DESC NULLS LAST) AS age_dense_rank
    FROM with_dims
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        tag,
        email,
        age,
        segment,
        segment_stddev_samp_age,
        age_dense_rank,
        TRIM(segment) AS clean_segment,
        COALESCE(tag, 'UNKNOWN') AS resolved_tag
    FROM with_windows
    WHERE segment IS NOT NULL
      AND segment_stddev_samp_age = 40
)
SELECT
    id,
    created_at,
    tag,
    email,
    segment_stddev_samp_age,
    age_dense_rank,
    clean_segment,
    resolved_tag,
    CASE WHEN tag RLIKE '^(BUNDLE|WELCOME5)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS tag_zone
FROM filtered
ORDER BY segment_stddev_samp_age DESC;
