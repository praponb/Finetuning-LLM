WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS val
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(currency) AS currency_upper,
        FLOOR(val) AS val_whole
    FROM exploded
    WHERE currency IN ('EUR', 'USD')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT currency
    FROM base
    EXCEPT
    SELECT currency
    FROM price_tiers
    WHERE effective_from >= '2023-11-21'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.currency = k.currency
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        val,
        val_whole,
        RANK() OVER w_grp AS val_whole_rank,
        PERCENT_RANK() OVER (ORDER BY val) AS val_percent_rank,
        RANK() OVER w_running AS val_whole_rank_2,
        RANK() OVER w_part AS val_whole_rank_3,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val) AS val_percent_rank_2
    FROM reconciled
    WHERE val <= 46
    WINDOW w_grp AS (ORDER BY val_whole NULLS FIRST), w_running AS (PARTITION BY currency ORDER BY val_whole NULLS LAST), w_part AS (PARTITION BY currency ORDER BY val_whole DESC NULLS LAST)
)
-- Ranking and gap features
SELECT
    val_whole,
    val_percent_rank,
    val_whole_rank_3,
    val_percent_rank_2,
    RANK() OVER w_grp AS val_percent_rank_2_rank,
    DENSE_RANK() OVER (ORDER BY val_whole) AS val_whole_dense_rank,
    DENSE_RANK() OVER (ORDER BY val_whole NULLS LAST) AS val_whole_dense_rank_2
FROM with_windows
WINDOW w_grp AS (ORDER BY val_percent_rank_2 DESC NULLS LAST)
