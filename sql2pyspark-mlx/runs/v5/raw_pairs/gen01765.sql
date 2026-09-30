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
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, attr_value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        code
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS code
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN code = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM lines_carts
    WHERE attr_key <> 'weight_kg'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        attr_key,
        is_vip,
        MIN(is_vip) OVER (PARTITION BY attr_key) AS attr_key_min_is_vip,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY is_vip NULLS LAST) AS is_vip_cume_dist,
        MAX(is_vip) OVER (PARTITION BY customer_id) AS customer_id_max_is_vip
    FROM base
    WHERE attr_key NOT IN ('device', 'weight_kg')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER w_running AS is_vip_cume_dist_cume_dist
    FROM enriched
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY is_vip_cume_dist DESC)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        attr_key,
        is_vip,
        is_vip_cume_dist,
        is_vip_cume_dist_cume_dist,
        is_vip_cume_dist + customer_id_max_is_vip AS is_vip_cume_dist_plus_customer_id_max_is_vip
    FROM scored
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'device'
)
-- Multi-level aggregation
SELECT
    attr_key,
    customer_id,
    CASE WHEN is_vip_cume_dist_cume_dist >= 0.63 THEN 'LARGE' ELSE 'SMALL' END AS is_vip_cume_dist_cume_dist_tier,
    COUNT_IF(attr_key = 'utm_source') AS utm_source_events,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(SUM(is_vip_cume_dist_cume_dist) / NULLIF(COUNT(CASE WHEN attr_key = 'gift' THEN 1 END), 0), 2) AS is_vip_cume_dist_cume_dist_per_gift,
    COUNT(CASE WHEN attr_key = 'utm_source' THEN 1 END) AS utm_source_count
FROM cleaned
GROUP BY attr_key, customer_id, CASE WHEN is_vip_cume_dist_cume_dist >= 0.63 THEN 'LARGE' ELSE 'SMALL' END;
