WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT coupon_codes, QUARTER(created_at) AS created_at_quarter
    FROM lines
    WHERE created_at > TO_TIMESTAMP('2025-03-01 14:00:00')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(created_at_quarter, 0) AS created_at_quarter_filled,
        CASE WHEN created_at_quarter >= 64 THEN 'High' WHEN created_at_quarter >= 23 THEN 'Medium' ELSE 'Low' END AS created_at_quarter_band
    FROM filtered
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        coupon_codes,
        created_at_quarter,
        created_at_quarter_band,
        DENSE_RANK() OVER (ORDER BY created_at_quarter_filled) AS created_at_quarter_filled_dense_rank,
        DENSE_RANK() OVER w_grp AS created_at_quarter_filled_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY created_at_quarter NULLS LAST) AS created_at_quarter_dense_rank
    FROM valid
    WINDOW w_grp AS (ORDER BY created_at_quarter_filled DESC NULLS FIRST)
)
SELECT
    coupon_codes,
    created_at_quarter,
    created_at_quarter_filled_dense_rank
FROM metrics;
