WITH flattened AS (
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS tier_value
),
matched_keys AS (
    SELECT currency, id
    FROM flattened
    EXCEPT ALL
    SELECT currency, id
    FROM price_tiers
    WHERE plan IN ('basic', 'enterprise')
),
kept_rows AS (
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.currency = k.currency AND s.id = k.id
),
metrics AS (
    SELECT
        id,
        currency,
        tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank
    FROM kept_rows
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(tier_value_cume_dist) OVER w_ordered AS currency_stddev_samp_tier_value_cume_dist,
        MAX(tier_value_cume_dist) OVER w_ordered AS currency_max_tier_value_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank DESC NULLS FIRST) AS tier_value_dense_rank_dense_rank,
        MAX(tier_value) OVER w_ordered AS currency_max_tier_value
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY currency)
)
SELECT
    CASE WHEN currency_max_tier_value >= 65 THEN 'HIGH' ELSE 'LOW' END AS currency_max_tier_value_tier,
    COUNT(*) AS event_count,
    ROUND(AVG(tier_value), 2) AS avg_tier_value
FROM with_windows
GROUP BY ROLLUP (CASE WHEN currency_max_tier_value >= 65 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY event_count DESC NULLS LAST
