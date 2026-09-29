WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS tier_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist_2,
        SUM(tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value,
        SUM(tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value_2
    FROM exploded
    WHERE currency IN ('THB', 'USD')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(tier_value_cume_dist AS INT) AS tier_value_cume_dist_int
    FROM with_windows
    WHERE currency_sum_tier_value BETWEEN 115 AND 396
),
filtered_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        tier_value,
        currency_min_tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        currency_sum_tier_value,
        LEAST(tier_value_cume_dist_int, tier_value) AS least_tier_value_cume_dist_int_tier_value
    FROM filtered
    WHERE tier_value_cume_dist BETWEEN 0.25 AND 0.54
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        tier_value_cume_dist_2,
        currency_sum_tier_value,
        CUME_DIST() OVER w_hist AS least_tier_value_cume_dist_int_tier_value_cume_dist,
        MIN(currency_min_tier_value) OVER w_hist_2 AS currency_min_currency_min_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_sum_tier_value NULLS LAST) AS currency_sum_tier_value_dense_rank
    FROM filtered_price_tiers
    WINDOW w_hist AS (PARTITION BY currency ORDER BY least_tier_value_cume_dist_int_tier_value DESC NULLS FIRST), w_hist_2 AS (PARTITION BY currency)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN least_tier_value_cume_dist_int_tier_value_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END AS least_tier_value_cume_dist_int_tier_value_cume_dist_tier,
    CASE WHEN currency_sum_tier_value >= 401 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_sum_tier_value_tier,
    COUNT(*) AS num_rows,
    SUM(currency_min_currency_min_tier_value) AS total_currency_min_currency_min_tier_value,
    APPROX_PERCENTILE(currency_min_currency_min_tier_value, ARRAY(0.25, 0.5, 0.75)) AS currency_min_currency_min_tier_value_quartiles
FROM scored
GROUP BY CUBE (CASE WHEN least_tier_value_cume_dist_int_tier_value_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN currency_sum_tier_value >= 401 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING num_rows > 1
ORDER BY total_currency_min_currency_min_tier_value DESC
