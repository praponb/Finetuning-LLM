WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, v
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        tag
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) li AS tag
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN k = 'weight_kg' THEN 1 ELSE 0 END AS is_weight_kg,
        COALESCE(tag, 'DIRECT') AS tag_filled
    FROM exploded
    WHERE k NOT IN ('gift', 'device')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        tag,
        is_weight_kg,
        tag_filled,
        MIN(is_weight_kg) OVER w_ordered AS k_min_is_weight_kg,
        CUME_DIST() OVER (ORDER BY is_weight_kg NULLS FIRST) AS is_weight_kg_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY is_weight_kg DESC NULLS FIRST) AS is_weight_kg_percent_rank
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY k)
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        k,
        tag,
        is_weight_kg,
        tag_filled,
        k_min_is_weight_kg,
        is_weight_kg_cume_dist,
        is_weight_kg_percent_rank,
        STDDEV_SAMP(k_min_is_weight_kg) OVER (PARTITION BY tag) AS tag_stddev_samp_k_min_is_weight_kg,
        SUM(is_weight_kg_percent_rank) OVER (PARTITION BY k) AS k_sum_is_weight_kg_percent_rank,
        COUNT(k_min_is_weight_kg) OVER (PARTITION BY k) AS k_count_k_min_is_weight_kg
    FROM scored
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(is_weight_kg) OVER (PARTITION BY k) AS k_max_is_weight_kg,
        STDDEV_SAMP(k_sum_is_weight_kg_percent_rank) OVER (PARTITION BY k) AS k_stddev_samp_k_sum_is_weight_kg_percent_rank
    FROM scored_carts
)
-- Multi-level aggregation
SELECT
    CASE WHEN k_stddev_samp_k_sum_is_weight_kg_percent_rank >= 3 THEN 'HIGH' ELSE 'LOW' END AS k_stddev_samp_k_sum_is_weight_kg_percent_rank_tier,
    tag,
    k,
    PERCENTILE_APPROX(is_weight_kg_cume_dist, 0.95) AS is_weight_kg_cume_dist_p95,
    COUNT(*) AS num_rows,
    AVG(k_max_is_weight_kg) AS avg_k_max_is_weight_kg
FROM with_windows
GROUP BY 1, 2, 3
HAVING PERCENTILE_APPROX(is_weight_kg_cume_dist, 0.95) < 0
ORDER BY avg_k_max_is_weight_kg DESC NULLS LAST;
