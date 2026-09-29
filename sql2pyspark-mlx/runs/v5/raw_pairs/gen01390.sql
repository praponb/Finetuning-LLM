WITH lines AS (
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS val
),
enriched AS (
    SELECT
        id,
        currency,
        val,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
        STDDEV_SAMP(val) OVER (PARTITION BY currency) AS currency_stddev_samp_val,
        COUNT(val) OVER (PARTITION BY currency) AS currency_count_val,
        CUME_DIST() OVER (ORDER BY val DESC NULLS FIRST) AS val_cume_dist
    FROM lines
),
filtered AS (
    SELECT *, val_percent_rank + currency_stddev_samp_val AS val_percent_rank_plus_currency_stddev_samp_val
    FROM enriched
    WHERE val_percent_rank >= 0.28
),
base AS (
    SELECT
        id,
        currency,
        currency_max_val,
        val_percent_rank,
        currency_count_val,
        val_cume_dist,
        CASE WHEN currency IN ('EUR', 'THB') THEN 'Tier 1' ELSE 'Other' END AS currency_group,
        val_percent_rank_plus_currency_stddev_samp_val + val_percent_rank AS val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank
    FROM filtered
    WHERE val_percent_rank < 0.35
),
enriched_price_tiers AS (
    SELECT
        currency_max_val,
        val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank,
        AVG(val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank) OVER (PARTITION BY currency) AS currency_avg_val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank
    FROM base
    WHERE val_cume_dist BETWEEN 0.56 AND 0.79
)
SELECT
    val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank,
    currency_avg_val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank,
    CUME_DIST() OVER (ORDER BY currency_max_val DESC) AS currency_max_val_cume_dist,
    DENSE_RANK() OVER (ORDER BY val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank DESC NULLS LAST) AS val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank_dense_rank
FROM enriched_price_tiers;
