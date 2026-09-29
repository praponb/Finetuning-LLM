WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS val
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(currency) currency_upper
    FROM flattened
    WHERE effective_from >= DATE '2024-02-27'
      AND currency = 'USD'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val NULLS LAST) val_percent_rank,
        MIN(val) OVER (PARTITION BY currency) currency_min_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC) val_percent_rank_2
    FROM cleaned
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(val) OVER (PARTITION BY currency) currency_min_val_2,
        MIN(val_percent_rank_2) OVER (PARTITION BY currency) currency_min_val_percent_rank_2,
        MAX(val_percent_rank_2) OVER (PARTITION BY currency) currency_max_val_percent_rank_2,
        PERCENT_RANK() OVER (ORDER BY val_percent_rank_2 DESC NULLS FIRST) val_percent_rank_2_percent_rank
    FROM metrics
    WHERE val BETWEEN 21 AND 59
),
scored AS (
    -- Running and moving aggregates
    SELECT
        currency,
        currency_min_val,
        val_percent_rank_2,
        currency_min_val_percent_rank_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_min_val_2 DESC NULLS LAST) currency_min_val_2_dense_rank,
        RANK() OVER (ORDER BY currency_max_val_percent_rank_2 DESC) currency_max_val_percent_rank_2_rank,
        CUME_DIST() OVER w_part val_percent_rank_cume_dist
    FROM with_windows
    WINDOW w_part AS (ORDER BY val_percent_rank)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency,
        currency_min_val,
        val_percent_rank_2,
        currency_min_val_percent_rank_2,
        currency_min_val_2_dense_rank,
        currency_max_val_percent_rank_2_rank,
        val_percent_rank_cume_dist,
        ROUND(val_percent_rank_2 / currency_min_val_2_dense_rank, 2) val_percent_rank_2_per_currency_min_val_2_dense_rank
    FROM scored
    WHERE currency_min_val_percent_rank_2 <= 0
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        currency,
        COUNT_IF(currency = 'USD') AS usd_events,
        AVG(val_percent_rank_2_per_currency_min_val_2_dense_rank) FILTER (WHERE currency = 'EUR') AS eur_avg_val_percent_rank_2_per_currency_min_val_2_dense_rank,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
    FROM valid
    GROUP BY currency
    HAVING usd_events >= 1
)
SELECT
    currency,
    usd_events,
    eur_avg_val_percent_rank_2_per_currency_min_val_2_dense_rank,
    CASE WHEN currency RLIKE '^(EUR|USD)$' THEN 'CORE' WHEN currency RLIKE '^(THB)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS currency_zone
FROM summary
ORDER BY currency
