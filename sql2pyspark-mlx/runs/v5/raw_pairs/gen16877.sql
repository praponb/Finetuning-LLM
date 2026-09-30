WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
enriched AS (
    SELECT
        id,
        currency,
        tier_value,
        STDDEV_SAMP(tier_value) OVER w_recent AS currency_stddev_samp_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank_2,
        DENSE_RANK() OVER w_hist AS tier_value_dense_rank_3,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist
    FROM lines
    WINDOW w_recent AS (PARTITION BY currency), w_hist AS (ORDER BY tier_value DESC)
),
cleaned AS (
    SELECT *, COALESCE(tier_value_dense_rank_3, 0) AS tier_value_dense_rank_3_filled
    FROM enriched
    WHERE tier_value_dense_rank_3 < 4
),
filtered AS (
    SELECT
        id,
        currency,
        tier_value,
        currency_stddev_samp_tier_value,
        tier_value_dense_rank,
        tier_value_dense_rank_3,
        tier_value_dense_rank / NULLIF(tier_value_dense_rank_3_filled, 0) AS tier_value_dense_rank_per_tier_value_dense_rank_3_filled,
        tier_value - currency_stddev_samp_tier_value AS tier_value_minus_currency_stddev_samp_tier_value
    FROM cleaned
    WHERE tier_value_dense_rank_3 > 5
),
scored AS (
    SELECT
        *,
        SUM(tier_value_dense_rank_per_tier_value_dense_rank_3_filled) OVER (PARTITION BY currency) AS currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled,
        STDDEV_SAMP(currency_stddev_samp_tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_currency_stddev_samp_tier_value
    FROM filtered
),
metrics AS (
    SELECT
        tier_value,
        currency_stddev_samp_tier_value,
        currency_stddev_samp_currency_stddev_samp_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled DESC NULLS FIRST) AS currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled_dense_rank,
        DENSE_RANK() OVER (ORDER BY currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled DESC NULLS FIRST) AS currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled_dense_rank_2,
        PERCENT_RANK() OVER (ORDER BY tier_value_dense_rank_3 DESC NULLS FIRST) AS tier_value_dense_rank_3_percent_rank,
        MIN(tier_value_minus_currency_stddev_samp_tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value_minus_currency_stddev_samp_tier_value,
        RANK() OVER (ORDER BY tier_value_dense_rank_per_tier_value_dense_rank_3_filled NULLS FIRST) AS tier_value_dense_rank_per_tier_value_dense_rank_3_filled_rank
    FROM scored
),
scored_price_tiers AS (
    SELECT
        currency_stddev_samp_tier_value,
        currency_stddev_samp_currency_stddev_samp_tier_value,
        currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled_dense_rank,
        currency_min_tier_value_minus_currency_stddev_samp_tier_value,
        CUME_DIST() OVER (ORDER BY tier_value_dense_rank_per_tier_value_dense_rank_3_filled_rank NULLS LAST) AS tier_value_dense_rank_per_tier_value_dense_rank_3_filled_rank_cume_dist,
        RANK() OVER (ORDER BY tier_value_dense_rank_3_percent_rank DESC NULLS FIRST) AS tier_value_dense_rank_3_percent_rank_rank
    FROM metrics
    WHERE currency_stddev_samp_currency_stddev_samp_tier_value >= 77
)
SELECT
    currency_stddev_samp_tier_value,
    currency_min_tier_value_minus_currency_stddev_samp_tier_value,
    tier_value_dense_rank_3_percent_rank_rank
FROM scored_price_tiers
