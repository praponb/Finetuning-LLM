WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, v
),
flattened_carts AS (
    SELECT
        id,
        created_at,
        key,
        v,
        code
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) li AS code
),
cleaned AS (
    SELECT
        id,
        key,
        v,
        LOWER(code) code_lower,
        INITCAP(TRIM(key)) key_title
    FROM flattened_carts
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 07:00:00')
      AND created_at < '2023-11-08'
),
cleaned_carts AS (
    SELECT
        *,
        CASE WHEN key IN ('weight_kg', 'gift') THEN 'Tier 1' ELSE 'Other' END key_group,
        CASE WHEN key = 'device' THEN 1 ELSE 0 END is_device,
        COALESCE(key, 'N/A') key_filled
    FROM cleaned
    WHERE key <> 'gift'
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(is_device) OVER (PARTITION BY key) key_stddev_samp_is_device,
        AVG(is_device) OVER (PARTITION BY key) key_avg_is_device
    FROM cleaned_carts
),
scored AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY key ORDER BY is_device DESC NULLS FIRST) is_device_rank,
        COUNT(key_avg_is_device) OVER w_running key_count_key_avg_is_device,
        MAX(key_stddev_samp_is_device) OVER w_running key_max_key_stddev_samp_is_device
    FROM metrics
    WINDOW w_running AS (PARTITION BY key)
),
scored_carts AS (
    SELECT
        *,
        RANK() OVER (ORDER BY key_stddev_samp_is_device NULLS FIRST) key_stddev_samp_is_device_rank,
        CUME_DIST() OVER (PARTITION BY key ORDER BY key_avg_is_device) key_avg_is_device_cume_dist,
        SUM(key_max_key_stddev_samp_is_device) OVER (PARTITION BY key) key_sum_key_max_key_stddev_samp_is_device
    FROM scored
),
stats AS (
    SELECT
        key,
        COUNT(*) FILTER (WHERE key = 'weight_kg') AS weight_kg_rows,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list
    FROM scored_carts
    GROUP BY 1
    HAVING COUNT(*) FILTER (WHERE key = 'weight_kg') >= 1
)
SELECT key, weight_kg_rows
FROM stats
ORDER BY weight_kg_rows DESC NULLS LAST
