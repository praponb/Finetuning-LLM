WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        line.qty,
        line.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE_OUTER(items) AS line
        FROM carts
    ) li
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        created_at,
        promo,
        LOWER(promo) AS promo_lower,
        COALESCE(promo, 'UNKNOWN') AS promo_filled
    FROM exploded
    WHERE created_at > DATE '2025-03-01'
      AND promo IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label,
        LOWER(promo) AS promo_lower_2,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM valid
    WHERE customer_id IS NOT NULL
      AND created_at >= '2023-05-01'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        created_at,
        promo,
        promo_lower,
        created_at_label,
        promo_lower_2,
        resolved_promo,
        CASE WHEN promo = 'VIP' THEN 1 ELSE 0 END AS is_vip
    FROM filtered
    WHERE customer_id IN (1, 17, 18, 25)
)
-- Multi-level aggregation
SELECT
    customer_id,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows,
    PERCENTILE_APPROX(is_vip, 0.95) AS is_vip_p95,
    COUNT(*) FILTER (WHERE promo = 'SPRING10') AS spring10_rows,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    APPROX_PERCENTILE(is_vip, ARRAY(0.25, 0.5, 0.75)) AS is_vip_quartiles,
    COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END) AS bundle_count
FROM base
GROUP BY customer_id
