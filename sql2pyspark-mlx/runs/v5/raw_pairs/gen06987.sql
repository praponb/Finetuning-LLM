WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, attr_value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_value,
        EXPLODE(coupon_codes) AS code
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5
    FROM lines_carts
    WHERE code IS NOT DISTINCT FROM 'VIP'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(is_welcome5) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_welcome5_max_1h,
        RANK() OVER w_grp AS is_welcome5_rank,
        STDDEV_SAMP(is_welcome5) OVER (PARTITION BY code) AS code_stddev_samp_is_welcome5
    FROM filtered
    WINDOW w_grp AS (PARTITION BY code ORDER BY is_welcome5 NULLS FIRST)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        code,
        is_welcome5_max_1h,
        code_stddev_samp_is_welcome5,
        CASE WHEN is_welcome5_max_1h >= 64 THEN 'High' WHEN is_welcome5_max_1h >= 24 THEN 'Medium' ELSE 'Low' END AS is_welcome5_max_1h_tier,
        LOWER(code) AS code_lower,
        COALESCE(code, 'NONE') AS code_clean
    FROM with_windows
    WHERE code IS DISTINCT FROM 'VIP'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN code_stddev_samp_is_welcome5 >= 86 THEN 'LARGE' ELSE 'SMALL' END AS code_stddev_samp_is_welcome5_tier,
        code,
        SORT_ARRAY(COLLECT_SET(code)) AS code_list,
        AVG(code_stddev_samp_is_welcome5) FILTER (WHERE code = 'VIP') AS vip_avg_code_stddev_samp_is_welcome5,
        GROUPING(CASE WHEN code_stddev_samp_is_welcome5 >= 86 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
    FROM valid
    GROUP BY ROLLUP (CASE WHEN code_stddev_samp_is_welcome5 >= 86 THEN 'LARGE' ELSE 'SMALL' END, code)
)
SELECT
    code_stddev_samp_is_welcome5_tier,
    code_list,
    vip_avg_code_stddev_samp_is_welcome5
FROM summary
ORDER BY code_stddev_samp_is_welcome5_tier NULLS LAST;
