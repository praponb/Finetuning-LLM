WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(plan) AS clean_plan,
        LOWER(currency) AS currency_lower
    FROM flattened
    WHERE effective_from > DATE '2024-05-22'
),
scored AS (
    -- Running and moving aggregates
    SELECT *, STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val
    FROM filtered
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(plan_stddev_samp_val) OVER (PARTITION BY currency) AS currency_min_plan_stddev_samp_val,
        RANK() OVER (PARTITION BY currency ORDER BY plan_stddev_samp_val DESC) AS plan_stddev_samp_val_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY val DESC NULLS LAST) AS val_cume_dist
    FROM scored
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        currency,
        clean_plan,
        plan_stddev_samp_val,
        currency_min_plan_stddev_samp_val,
        plan_stddev_samp_val_rank,
        RANK() OVER (ORDER BY val_cume_dist ASC) AS val_cume_dist_rank,
        COUNT(val_cume_dist) OVER (PARTITION BY plan) AS plan_count_val_cume_dist
    FROM enriched
),
scored_price_tiers_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        clean_plan,
        currency_min_plan_stddev_samp_val,
        plan_stddev_samp_val_rank,
        val_cume_dist_rank,
        plan_count_val_cume_dist,
        AVG(plan_stddev_samp_val) OVER (PARTITION BY currency) AS currency_avg_plan_stddev_samp_val,
        ROUND(val_cume_dist_rank / SUM(val_cume_dist_rank) OVER (PARTITION BY currency), 4) AS val_cume_dist_rank_share,
        MAX(currency_min_plan_stddev_samp_val) OVER (PARTITION BY currency) AS currency_max_currency_min_plan_stddev_samp_val
    FROM scored_price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(currency_max_currency_min_plan_stddev_samp_val, val_cume_dist_rank_share) AS least_currency_max_currency_min_plan_stddev_samp_val_val_cume_dist_rank_share
    FROM scored_price_tiers_2
    WHERE effective_from > DATE '2024-02-12'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        currency,
        DATE_TRUNC('WEEK', effective_from) AS week_start,
        COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
        SUM(CASE WHEN currency = 'USD' THEN currency_min_plan_stddev_samp_val ELSE 0 END) AS usd_currency_min_plan_stddev_samp_val,
        COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows
    FROM valid
    GROUP BY currency, DATE_TRUNC('WEEK', effective_from)
)
SELECT
    currency,
    week_start,
    thb_count,
    eur_rows
FROM totals;
