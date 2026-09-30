WITH flattened AS (
    SELECT
        id,
        effective_from,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
filtered AS (
    SELECT *, COALESCE(tier_value, 0) AS tier_value_filled
    FROM flattened
    WHERE effective_from > TO_DATE('2024-06-09')
      AND MONTH(effective_from) IN (1, 3, 5)
),
filtered_price_tiers AS (
    SELECT *, tier_value + tier_value_filled AS tier_value_plus_tier_value_filled
    FROM filtered
    WHERE effective_from > DATE '2024-02-07'
      AND tier_value >= 65
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_filled) AS tier_value_filled_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist
    FROM filtered_price_tiers
    WHERE tier_value_plus_tier_value_filled > 40
),
kept_keys AS (
    SELECT id
    FROM enriched
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE plan != 'pro'
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered_price_tiers_2 AS (
    SELECT *, DATE_ADD(effective_from, 59) AS effective_from_plus_59d
    FROM verified
    WHERE effective_from > TO_DATE('2024-06-09')
),
scored AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY tier_value_plus_tier_value_filled) AS tier_value_plus_tier_value_filled_dense_rank
    FROM filtered_price_tiers_2
)
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    CASE WHEN tier_value_cume_dist >= 0.74 THEN 'HIGH' ELSE 'LOW' END AS tier_value_cume_dist_tier,
    CASE WHEN tier_value_plus_tier_value_filled_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_plus_tier_value_filled_dense_rank_tier,
    APPROX_PERCENTILE(tier_value_filled_cume_dist, 0.95) AS tier_value_filled_cume_dist_p95,
    APPROX_PERCENTILE(tier_value_filled_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS tier_value_filled_cume_dist_quartiles,
    MIN(tier_value_plus_tier_value_filled_dense_rank) AS min_tier_value_plus_tier_value_filled_dense_rank,
    COUNT(*) AS event_count
FROM scored
GROUP BY ROLLUP (DATE_TRUNC('MONTH', effective_from), CASE WHEN tier_value_cume_dist >= 0.74 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_plus_tier_value_filled_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING tier_value_filled_cume_dist_p95 < 1000000
ORDER BY activity_month DESC
