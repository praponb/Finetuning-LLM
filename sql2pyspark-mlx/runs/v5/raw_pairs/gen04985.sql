WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS val
),
enriched AS (
    -- Ranking and gap features
    SELECT
        currency,
        val,
        RANK() OVER w_part AS val_rank,
        SUM(val) OVER w_running AS currency_sum_val,
        MIN(val) OVER w_running AS currency_min_val
    FROM lines
    WHERE currency NOT IN ('THB', 'USD')
    WINDOW w_part AS (PARTITION BY currency ORDER BY val NULLS LAST), w_running AS (PARTITION BY currency)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        val,
        val_rank,
        RANK() OVER (ORDER BY currency_min_val DESC NULLS LAST) AS currency_min_val_rank,
        COUNT(currency_sum_val) OVER (PARTITION BY currency) AS currency_count_currency_sum_val,
        STDDEV_SAMP(val) OVER (PARTITION BY currency) AS currency_stddev_samp_val,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_dense_rank
    FROM enriched
),
kept_keys AS (
    -- Set intersection on keys
    SELECT currency
    FROM scored
    INTERSECT
    SELECT currency
    FROM price_tiers
    WHERE effective_from < '2024-07-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.currency = k.currency
)
-- Multi-level aggregation
SELECT
    currency,
    CASE WHEN currency_count_currency_sum_val >= 13 THEN 'LARGE' ELSE 'SMALL' END AS currency_count_currency_sum_val_tier,
    MAX(currency_min_val_rank) AS max_currency_min_val_rank,
    COUNT(DISTINCT currency) AS distinct_currencies,
    COUNT(DISTINCT currency) AS distinct_currencies_2,
    MAX(currency_count_currency_sum_val) AS max_currency_count_currency_sum_val,
    COUNT(*) AS num_rows,
    AVG(val_rank) FILTER (WHERE currency = 'USD') AS usd_avg_val_rank,
    GROUPING(currency) AS gid
FROM kept_rows
GROUP BY GROUPING SETS ((currency, CASE WHEN currency_count_currency_sum_val >= 13 THEN 'LARGE' ELSE 'SMALL' END), (currency))
ORDER BY currency_count_currency_sum_val_tier DESC NULLS FIRST
