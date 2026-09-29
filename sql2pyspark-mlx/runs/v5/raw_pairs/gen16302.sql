WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS val
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(val) val_whole,
        SUBSTRING(currency, 1, 4) currency_prefix
    FROM exploded
    WHERE plan IN ('enterprise', 'basic')
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        currency,
        val_whole,
        currency_prefix,
        INITCAP(TRIM(plan)) plan_title,
        UPPER(plan) plan_upper
    FROM valid
    WHERE val_whole > 43
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        val_whole,
        currency_prefix,
        plan_title,
        MIN(val_whole) OVER w_grp currency_min_val_whole,
        SUM(val_whole) OVER w_grp currency_sum_val_whole
    FROM valid_price_tiers
    WINDOW w_grp AS (PARTITION BY currency)
),
valid_price_tiers_2 AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN currency_sum_val_whole >= 341 THEN 'High' WHEN currency_sum_val_whole >= 126 THEN 'Medium' ELSE 'Low' END currency_sum_val_whole_tier
    FROM enriched
    WHERE val_whole <= 39
),
valid_price_tiers_3 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        val_whole,
        plan_title,
        currency_min_val_whole,
        currency_sum_val_whole,
        val_whole - currency_min_val_whole val_whole_minus_currency_min_val_whole
    FROM valid_price_tiers_2
    WHERE val_whole > 32
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        val_whole,
        plan_title,
        CUME_DIST() OVER w_grp currency_sum_val_whole_cume_dist,
        RANK() OVER w_grp currency_sum_val_whole_rank,
        DENSE_RANK() OVER (ORDER BY currency_sum_val_whole DESC NULLS FIRST) currency_sum_val_whole_dense_rank,
        PERCENT_RANK() OVER w_grp_2 val_whole_minus_currency_min_val_whole_percent_rank
    FROM valid_price_tiers_3
    WINDOW w_grp AS (ORDER BY currency_sum_val_whole DESC NULLS LAST), w_grp_2 AS (ORDER BY val_whole_minus_currency_min_val_whole DESC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN val_whole_minus_currency_min_val_whole_percent_rank >= 0.85 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_whole_minus_currency_min_val_whole_percent_rank_tier,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    ROUND(AVG(currency_sum_val_whole_rank), 2) AS avg_currency_sum_val_whole_rank,
    MIN(currency_sum_val_whole_rank) AS min_currency_sum_val_whole_rank,
    COUNT(*) AS num_rows_2,
    COUNT(*) AS cnt
FROM metrics
GROUP BY CASE WHEN val_whole_minus_currency_min_val_whole_percent_rank >= 0.85 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY min_currency_sum_val_whole_rank DESC NULLS FIRST;
