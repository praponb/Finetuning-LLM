WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY val NULLS FIRST) AS val_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank,
        RANK() OVER (PARTITION BY plan, currency ORDER BY val DESC NULLS FIRST) AS val_rank_2,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY val DESC NULLS FIRST) AS val_dense_rank
    FROM lines
    WHERE plan = 'starter'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        val_rank,
        val_rank_2,
        val_dense_rank,
        CAST(val_rank_2 AS STRING) AS val_rank_2_str
    FROM with_windows
    WHERE val_dense_rank < 7
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        val_rank_2,
        val_dense_rank,
        val_rank_2_str,
        val_rank_2 + val_dense_rank AS val_rank_2_plus_val_dense_rank
    FROM cleaned
    WHERE plan != 'basic'
)
-- Multi-level aggregation
SELECT
    plan,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count
FROM filtered
GROUP BY plan
HAVING COUNT(*) > 1
ORDER BY plan NULLS FIRST;
