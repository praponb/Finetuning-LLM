WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        QUARTER(created_at) AS created_at_quarter,
        CASE WHEN element = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 01:00:00'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.created_at_quarter,
        e.is_spring10,
        d.age,
        d.signup_date,
        d.credit_limit
    FROM filtered e
    LEFT JOIN customers d
        ON e.customer_id = d.id
    WHERE d.is_active = FALSE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, created_at_quarter - is_spring10 AS created_at_quarter_minus_is_spring10
    FROM with_dims
    WHERE created_at >= DATE '2025-03-01'
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(created_at_quarter) OVER (PARTITION BY customer_id) AS customer_id_min_created_at_quarter,
        CUME_DIST() OVER w_grp AS created_at_quarter_cume_dist,
        DENSE_RANK() OVER (ORDER BY age DESC NULLS LAST) AS age_dense_rank
    FROM cleaned
    WINDOW w_grp AS (ORDER BY created_at_quarter DESC NULLS LAST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        created_at,
        age,
        created_at_quarter_minus_is_spring10,
        customer_id_min_created_at_quarter,
        age_dense_rank,
        MAX(created_at_quarter) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_quarter_max_2h,
        STDDEV_SAMP(age_dense_rank) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_age_dense_rank,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY created_at_quarter_minus_is_spring10 NULLS LAST) AS created_at_quarter_minus_is_spring10_cume_dist,
        RANK() OVER (PARTITION BY customer_id ORDER BY customer_id_min_created_at_quarter DESC) AS customer_id_min_created_at_quarter_rank,
        DENSE_RANK() OVER (ORDER BY created_at_quarter) AS created_at_quarter_dense_rank
    FROM with_windows
)
SELECT
    id,
    customer_id,
    created_at,
    age,
    customer_id_min_created_at_quarter,
    created_at_quarter_max_2h,
    created_at_quarter_minus_is_spring10_cume_dist,
    customer_id_min_created_at_quarter_rank,
    created_at_quarter_dense_rank
FROM metrics
ORDER BY created_at_quarter_max_2h NULLS FIRST
