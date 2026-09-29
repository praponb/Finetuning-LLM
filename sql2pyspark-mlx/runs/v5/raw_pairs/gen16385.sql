WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS k, v
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        CASE WHEN k = 'gift' THEN 1 ELSE 0 END AS is_gift
    FROM lines
    WHERE k IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        k,
        v,
        is_gift,
        ROUND(is_gift * 1.15, 2) AS is_gift_adjusted,
        COALESCE(k, 'OTHER') AS k_clean
    FROM cleaned
    WHERE is_gift BETWEEN 40 AND 41
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM carts
    WHERE customer_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(k)) AS k_title
    FROM kept_rows
    WHERE k IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        k,
        k_clean,
        CUME_DIST() OVER (ORDER BY is_gift DESC) AS is_gift_cume_dist,
        PERCENT_RANK() OVER (ORDER BY is_gift_adjusted DESC NULLS LAST) AS is_gift_adjusted_percent_rank,
        COUNT(is_gift) OVER (PARTITION BY k) AS k_count_is_gift,
        AVG(is_gift) OVER (PARTITION BY k) AS k_avg_is_gift
    FROM cleaned_carts
    WHERE is_gift_adjusted < 38
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_recent AS is_gift_adjusted_percent_rank_rank,
        PERCENT_RANK() OVER (ORDER BY k_count_is_gift NULLS LAST) AS k_count_is_gift_percent_rank,
        DENSE_RANK() OVER w_hist AS k_count_is_gift_dense_rank,
        STDDEV_SAMP(is_gift_cume_dist) OVER (PARTITION BY k) AS k_stddev_samp_is_gift_cume_dist
    FROM enriched
    WINDOW w_recent AS (PARTITION BY k ORDER BY is_gift_adjusted_percent_rank NULLS FIRST), w_hist AS (PARTITION BY k ORDER BY k_count_is_gift NULLS FIRST)
)
SELECT
    k_clean,
    is_gift_cume_dist,
    is_gift_adjusted_percent_rank,
    k_count_is_gift,
    k_avg_is_gift,
    is_gift_adjusted_percent_rank_rank,
    k_count_is_gift_dense_rank,
    k_stddev_samp_is_gift_cume_dist
FROM metrics
