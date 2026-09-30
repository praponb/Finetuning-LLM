WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY val ASC NULLS FIRST) AS val_percent_rank,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val ASC NULLS FIRST) AS val_percent_rank_2
    FROM exploded
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        val_percent_rank,
        plan_count_val,
        val_percent_rank_2,
        GREATEST(val, val_percent_rank_2) AS greatest_val_val_percent_rank_2,
        CASE WHEN plan_count_val >= 16 THEN 'High' WHEN plan_count_val >= 7 THEN 'Medium' ELSE 'Low' END AS plan_count_val_level
    FROM with_windows
    WHERE effective_from >= DATE '2024-04-30'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        val_percent_rank,
        val_percent_rank_2,
        greatest_val_val_percent_rank_2,
        CEIL(val) AS val_whole
    FROM filtered
    WHERE effective_from >= DATE '2024-02-22'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        greatest_val_val_percent_rank_2,
        val_whole,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY greatest_val_val_percent_rank_2 ASC NULLS LAST) AS greatest_val_val_percent_rank_2_percent_rank,
        SUM(val_whole) OVER (PARTITION BY currency) AS currency_sum_val_whole,
        COUNT(greatest_val_val_percent_rank_2) OVER (PARTITION BY currency) AS currency_count_greatest_val_val_percent_rank_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank ASC NULLS LAST) AS val_percent_rank_dense_rank
    FROM cleaned
    WHERE val_percent_rank_2 BETWEEN 0.56 AND 0.72
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(greatest_val_val_percent_rank_2) OVER (PARTITION BY currency) AS currency_sum_greatest_val_val_percent_rank_2,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank_dense_rank ASC NULLS FIRST) AS val_percent_rank_dense_rank_percent_rank,
        RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank_dense_rank DESC NULLS LAST) AS val_percent_rank_dense_rank_rank
    FROM scored
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN val_percent_rank_dense_rank_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_percent_rank_dense_rank_rank_tier,
        COUNT_IF(currency = 'EUR') AS eur_events,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
    FROM enriched
    GROUP BY val_percent_rank_dense_rank_rank_tier
    HAVING COUNT_IF(currency = 'EUR') < 1000
)
SELECT
    val_percent_rank_dense_rank_rank_tier,
    eur_events,
    currency_list
FROM stats
