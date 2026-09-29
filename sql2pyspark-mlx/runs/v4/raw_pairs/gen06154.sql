WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS key, v
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS code
    FROM flattened
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(code) code_length,
        LENGTH(code) code_length_2
    FROM exploded
    WHERE code IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(code_length_2) OVER (PARTITION BY code) code_max_code_length_2,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY code_length DESC NULLS FIRST) code_length_dense_rank
    FROM valid
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(code_length) OVER w_ordered code_avg_code_length,
        STDDEV_SAMP(code_length_2) OVER w_ordered code_stddev_samp_code_length_2
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY code)
),
enriched_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        code,
        code_length,
        code_length_2,
        code_max_code_length_2,
        code_length_dense_rank,
        code_avg_code_length,
        code_stddev_samp_code_length_2,
        ROUND(code_length_dense_rank / SUM(code_length_dense_rank) OVER (PARTITION BY code), 4) code_length_dense_rank_share,
        COUNT(code_avg_code_length) OVER (PARTITION BY code) code_count_code_avg_code_length,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY code_max_code_length_2 DESC) code_max_code_length_2_dense_rank,
        MAX(code_stddev_samp_code_length_2) OVER (PARTITION BY code) code_max_code_stddev_samp_code_length_2,
        DENSE_RANK() OVER (ORDER BY code_length_2 NULLS LAST) code_length_2_dense_rank
    FROM metrics
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched_carts
    INTERSECT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_carts s
    JOIN remaining_keys k ON s.id = k.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN code_max_code_length_2 >= 90 THEN 'LARGE' ELSE 'SMALL' END AS code_max_code_length_2_tier,
        COUNT_IF(code = 'SPRING10') AS spring10_events,
        SUM(code_stddev_samp_code_length_2) AS total_code_stddev_samp_code_length_2,
        COUNT_IF(code = 'SPRING10') AS spring10_events_2,
        PERCENTILE_APPROX(code_avg_code_length, 0.95) AS code_avg_code_length_p95,
        SIZE(COLLECT_SET(code)) AS num_codes,
        COUNT(*) AS row_count
    FROM reconciled
    GROUP BY code_max_code_length_2_tier
)
SELECT
    code_max_code_length_2_tier,
    spring10_events,
    total_code_stddev_samp_code_length_2,
    spring10_events_2,
    code_avg_code_length_p95
FROM grouped;
