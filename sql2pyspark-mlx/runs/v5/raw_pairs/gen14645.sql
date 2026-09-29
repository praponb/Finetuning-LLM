WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank_2
    FROM lines
),
cleaned AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        currency_stddev_samp_tier_value,
        LEAST(tier_value_cume_dist, tier_value_rank) AS least_tier_value_cume_dist_tier_value_rank,
        currency_stddev_samp_tier_value - tier_value_cume_dist AS currency_stddev_samp_tier_value_minus_tier_value_cume_dist
    FROM with_windows
    WHERE currency_stddev_samp_tier_value <> 58
),
valid AS (
    SELECT *, currency_stddev_samp_tier_value - least_tier_value_cume_dist_tier_value_rank AS currency_stddev_samp_tier_value_minus_least_tier_value_cume_dist_tier_value_rank
    FROM cleaned
    WHERE effective_from > DATE '2024-02-11'
      AND tier_value >= 55
),
metrics AS (
    SELECT
        id,
        tier_value,
        currency_stddev_samp_tier_value,
        least_tier_value_cume_dist_tier_value_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_tier_value_minus_tier_value_cume_dist DESC NULLS LAST) AS currency_stddev_samp_tier_value_minus_tier_value_cume_dist_dense_rank,
        STDDEV_SAMP(currency_stddev_samp_tier_value) OVER w_hist AS currency_stddev_samp_currency_stddev_samp_tier_value
    FROM valid
    WINDOW w_hist AS (PARTITION BY currency)
)
SELECT
    CASE WHEN currency_stddev_samp_currency_stddev_samp_tier_value >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_stddev_samp_currency_stddev_samp_tier_value_tier,
    CASE WHEN tier_value >= 69 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    MIN(currency_stddev_samp_currency_stddev_samp_tier_value) AS min_currency_stddev_samp_currency_stddev_samp_tier_value,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM metrics
GROUP BY CASE WHEN currency_stddev_samp_currency_stddev_samp_tier_value >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN tier_value >= 69 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids NULLS LAST;
