WITH lines AS (
    SELECT
        id,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
),
base AS (
    SELECT *, CASE WHEN val >= 66 THEN 'High' WHEN val >= 33 THEN 'Medium' ELSE 'Low' END AS val_tier
    FROM lines
    WHERE currency IN ('THB', 'USD')
),
cleaned AS (
    SELECT
        *,
        ROUND(val, 0) AS val_rounded,
        CASE WHEN currency IN ('USD', 'EUR') THEN 'Group A' ELSE 'Other' END AS currency_group
    FROM base
    WHERE currency <> 'EUR'
),
valid AS (
    SELECT *, val * val_rounded AS val_times_val_rounded
    FROM cleaned
    WHERE val <= 27
),
with_windows AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY currency ORDER BY val_rounded DESC NULLS LAST) AS val_rounded_cume_dist
    FROM valid
)
SELECT
    val,
    val_rounded,
    currency_group,
    val_times_val_rounded,
    val_rounded_cume_dist,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY val_rounded_cume_dist NULLS FIRST) AS val_rounded_cume_dist_cume_dist,
    RANK() OVER (PARTITION BY currency ORDER BY val_rounded DESC NULLS FIRST) AS val_rounded_rank,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_rounded_cume_dist DESC) AS val_rounded_cume_dist_percent_rank,
    SUM(val_rounded_cume_dist) OVER w_recent AS currency_sum_val_rounded_cume_dist
FROM with_windows
WHERE val_rounded_cume_dist < 0.57
WINDOW w_recent AS (PARTITION BY currency)
