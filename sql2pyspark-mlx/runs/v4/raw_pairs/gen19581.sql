WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, currency
    FROM exploded
    INTERSECT
    SELECT id, currency
    FROM price_tiers
    WHERE YEAR(effective_from) >= 2025
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN matched_keys k ON s.id = k.id AND s.currency = k.currency
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        tier_value,
        STDDEV_SAMP(tier_value) OVER w_ordered AS currency_stddev_samp_tier_value,
        MIN(tier_value) OVER w_ordered AS currency_min_tier_value
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY currency)
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        tier_value,
        currency_stddev_samp_tier_value,
        currency_min_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY currency_stddev_samp_tier_value DESC) AS currency_stddev_samp_tier_value_dense_rank,
        CUME_DIST() OVER (ORDER BY currency_stddev_samp_tier_value NULLS FIRST) AS currency_stddev_samp_tier_value_cume_dist,
        DENSE_RANK() OVER (ORDER BY tier_value DESC) AS tier_value_dense_rank,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY currency), 4) AS tier_value_rank_share
    FROM enriched_price_tiers
),
enriched_price_tiers_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(currency_min_tier_value) OVER w_running AS currency_avg_currency_min_tier_value,
        COUNT(currency_min_tier_value) OVER w_running AS currency_count_currency_min_tier_value
    FROM metrics
    WINDOW w_running AS (PARTITION BY currency)
)
SELECT
    id,
    currency,
    currency_stddev_samp_tier_value,
    currency_min_tier_value,
    tier_value_rank,
    currency_stddev_samp_tier_value_dense_rank,
    tier_value_rank_share,
    currency_avg_currency_min_tier_value,
    currency_count_currency_min_tier_value,
    CASE WHEN currency RLIKE '^(THB|USD)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS currency_zone
FROM enriched_price_tiers_2;
