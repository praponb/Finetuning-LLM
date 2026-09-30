WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_part AS val_rank,
        PERCENT_RANK() OVER w_running AS val_percent_rank,
        DENSE_RANK() OVER w_part AS val_dense_rank
    FROM exploded
    WINDOW w_part AS (PARTITION BY currency ORDER BY val DESC NULLS FIRST), w_running AS (ORDER BY val NULLS FIRST)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank DESC) AS val_percent_rank_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_percent_rank DESC NULLS FIRST) AS val_percent_rank_cume_dist
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        currency,
        val_percent_rank_cume_dist,
        val
    FROM with_windows
) src
UNPIVOT (
    value FOR kpi IN (val_percent_rank_cume_dist, val)
)
