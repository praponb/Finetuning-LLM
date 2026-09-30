WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS line
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        qty,
        element
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN qty >= 4 THEN 'High' WHEN qty >= 3 THEN 'Medium' ELSE 'Low' END qty_band
    FROM flattened
    WHERE element IS NOT NULL
      AND element = 'BUNDLE'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(qty AS STRING) qty_str,
        CASE WHEN element = 'VIP' THEN 1 ELSE 0 END is_vip,
        COALESCE(element, 'N/A') element_clean
    FROM filtered
    WHERE element IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY qty NULLS LAST) qty_percent_rank,
        CUME_DIST() OVER (PARTITION BY element ORDER BY qty DESC NULLS FIRST) qty_cume_dist,
        SUM(is_vip) OVER (PARTITION BY element) element_sum_is_vip
    FROM base
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        qty,
        element,
        qty_band,
        qty_str,
        is_vip,
        element_clean,
        qty_percent_rank,
        qty_cume_dist,
        element_sum_is_vip,
        MAX(is_vip) OVER (PARTITION BY element) element_max_is_vip,
        DENSE_RANK() OVER (ORDER BY element_sum_is_vip DESC NULLS FIRST) element_sum_is_vip_dense_rank
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN qty_cume_dist >= 0.68 THEN 'HIGH' ELSE 'LOW' END AS qty_cume_dist_tier,
    element,
    COUNT(*) FILTER (WHERE element = 'WELCOME5') AS welcome5_rows,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY CASE WHEN qty_cume_dist >= 0.68 THEN 'HIGH' ELSE 'LOW' END, element
