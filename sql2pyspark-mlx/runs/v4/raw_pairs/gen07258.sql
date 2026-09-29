WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_dense_rank,
        AVG(element) OVER (PARTITION BY currency) AS currency_avg_element,
        RANK() OVER w_grp AS element_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_dense_rank_2
    FROM flattened
    WINDOW w_grp AS (PARTITION BY currency ORDER BY element NULLS LAST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_hist AS element_rank_rank,
        ROUND(element_dense_rank_2 / SUM(element_dense_rank_2) OVER (PARTITION BY currency), 4) AS element_dense_rank_2_share
    FROM scored
    WINDOW w_hist AS (PARTITION BY currency ORDER BY element_rank DESC NULLS LAST)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT currency
    FROM metrics
    INTERSECT
    SELECT currency
    FROM price_tiers
    WHERE plan IN ('basic', 'enterprise', 'pro')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.currency = k.currency
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        element,
        element_dense_rank,
        currency_avg_element,
        element_rank,
        element_dense_rank_2,
        element_rank_rank,
        element_dense_rank_2_share,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_dense_rank_3,
        STDDEV_SAMP(element_dense_rank_2) OVER (PARTITION BY currency) AS currency_stddev_samp_element_dense_rank_2,
        SUM(element) OVER (PARTITION BY currency) AS currency_sum_element,
        RANK() OVER (PARTITION BY currency ORDER BY element_dense_rank_2 DESC NULLS FIRST) AS element_dense_rank_2_rank
    FROM kept_rows
),
scored_price_tiers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        element,
        element_dense_rank,
        currency_avg_element,
        element_rank,
        element_dense_rank_2,
        element_rank_rank,
        element_dense_rank_2_share,
        element_dense_rank_3,
        currency_stddev_samp_element_dense_rank_2,
        currency_sum_element,
        element_dense_rank_2_rank,
        MAX(element_dense_rank_2_share) OVER w_running AS currency_max_element_dense_rank_2_share,
        MAX(element_dense_rank_3) OVER w_running AS currency_max_element_dense_rank_3,
        COUNT(element) OVER w_running AS currency_count_element,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_sum_element) AS currency_sum_element_cume_dist,
        RANK() OVER (PARTITION BY currency ORDER BY currency_avg_element DESC) AS currency_avg_element_rank
    FROM scored_price_tiers
    WINDOW w_running AS (PARTITION BY currency)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        element,
        element_dense_rank,
        currency_avg_element,
        element_dense_rank_2,
        element_dense_rank_2_share,
        currency_stddev_samp_element_dense_rank_2,
        currency_sum_element,
        element_dense_rank_2_rank,
        currency_max_element_dense_rank_2_share,
        currency_max_element_dense_rank_3,
        currency_count_element,
        currency_avg_element_rank,
        element_dense_rank / NULLIF(element_dense_rank_2_share, 0) AS element_dense_rank_per_element_dense_rank_2_share
    FROM scored_price_tiers_2
    WHERE currency_max_element_dense_rank_3 != 4
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN currency_sum_element >= 370 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_sum_element_tier,
        COUNT_IF(currency = 'USD') AS usd_events,
        PERCENTILE_APPROX(element_dense_rank_2_share, 0.9) AS element_dense_rank_2_share_p90
    FROM base
    GROUP BY ROLLUP (CASE WHEN currency_sum_element >= 370 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    currency_sum_element_tier,
    usd_events,
    element_dense_rank_2_share_p90
FROM totals
ORDER BY usd_events DESC NULLS FIRST, currency_sum_element_tier;
