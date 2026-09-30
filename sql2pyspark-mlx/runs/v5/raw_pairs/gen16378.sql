WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS val
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        val,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_dense_rank_2,
        RANK() OVER (ORDER BY val DESC) AS val_rank,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val NULLS LAST) AS val_percent_rank
    FROM lines
    WHERE val BETWEEN 25 AND 62
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY val_rank NULLS LAST) AS val_rank_rank,
        SUM(val_dense_rank_2) OVER w_ordered AS currency_sum_val_dense_rank_2
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY currency)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        val,
        val_dense_rank_2,
        val_rank,
        val_percent_rank,
        val_rank_rank,
        currency_sum_val_dense_rank_2,
        FLOOR(val_percent_rank) AS val_percent_rank_whole
    FROM metrics_price_tiers
    WHERE val_dense_rank_2 BETWEEN 4 AND 7
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY currency_sum_val_dense_rank_2 DESC NULLS FIRST) AS currency_sum_val_dense_rank_2_cume_dist,
        PERCENT_RANK() OVER w_running AS val_dense_rank_2_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank_2 NULLS FIRST) AS val_dense_rank_2_percent_rank_2
    FROM cleaned
    WINDOW w_running AS (ORDER BY val_dense_rank_2 DESC)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN currency_sum_val_dense_rank_2_cume_dist >= 0.85 THEN 'High' WHEN currency_sum_val_dense_rank_2_cume_dist >= 0.26 THEN 'Medium' ELSE 'Low' END AS currency_sum_val_dense_rank_2_cume_dist_level
    FROM scored
    WHERE currency_sum_val_dense_rank_2_cume_dist BETWEEN 0.24 AND 0.33
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN val_percent_rank_whole >= 83 THEN 'HIGH' ELSE 'LOW' END AS val_percent_rank_whole_tier,
    CASE WHEN val_dense_rank_2_percent_rank >= 0.71 THEN 'HIGH' ELSE 'LOW' END AS val_dense_rank_2_percent_rank_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(currency_sum_val_dense_rank_2_cume_dist) AS min_currency_sum_val_dense_rank_2_cume_dist,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM valid
GROUP BY CASE WHEN val_percent_rank_whole >= 83 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN val_dense_rank_2_percent_rank >= 0.71 THEN 'HIGH' ELSE 'LOW' END
ORDER BY val_percent_rank_whole_tier DESC NULLS LAST;
