WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        product_id,
        code
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        created_at,
        product_id,
        code,
        AVG(product_id) OVER w_part AS product_id_avg_2h,
        MIN(product_id) OVER w_ordered AS code_min_product_id,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY code), 4) AS product_id_share,
        COUNT(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_count_15m,
        MAX(product_id) OVER w_ordered AS code_max_product_id
    FROM lines_carts
    WINDOW w_part AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY code)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(product_id_share, 0) AS product_id_share_rounded
    FROM metrics
    WHERE product_id_count_15m = 13
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(product_id_count_15m) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_count_15m_count_1h,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY code), 4) AS product_id_share_2,
        ROUND(code_min_product_id / SUM(code_min_product_id) OVER (PARTITION BY code), 4) AS code_min_product_id_share,
        STDDEV_SAMP(product_id_avg_2h) OVER w_ordered AS code_stddev_samp_product_id_avg_2h,
        MIN(product_id) OVER w_ordered AS code_min_product_id_2
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY code)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        code_min_product_id,
        product_id_share_rounded,
        product_id_count_15m_count_1h,
        product_id_share_2,
        code_min_product_id_share,
        code_stddev_samp_product_id_avg_2h,
        code_min_product_id_2,
        COALESCE(code_stddev_samp_product_id_avg_2h, 0) AS code_stddev_samp_product_id_avg_2h_filled,
        CAST(product_id_share_2 AS INT) AS product_id_share_2_int,
        COALESCE(code, 'UNKNOWN') AS code_clean
    FROM scored
    WHERE created_at >= DATE '2025-03-01'
      AND code_stddev_samp_product_id_avg_2h > 5
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        created_at,
        code_min_product_id,
        product_id_share_rounded,
        product_id_share_2,
        code_min_product_id_share,
        code_min_product_id_2,
        code_stddev_samp_product_id_avg_2h_filled,
        product_id_share_2_int,
        HOUR(created_at) AS created_at_hour
    FROM valid
    WHERE created_at > DATE '2025-03-02'
)
-- Running and moving aggregates
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY product_id_share_rounded DESC NULLS FIRST) AS product_id_share_rounded_percent_rank,
    COUNT(product_id_share_2_int) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_share_2_int_count_1h,
    SUM(product_id_share_2_int) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_share_2_int_sum_30m,
    RANK() OVER w_running AS code_min_product_id_2_rank
FROM valid_carts
WINDOW w_running AS (ORDER BY code_min_product_id_2 DESC NULLS LAST);
