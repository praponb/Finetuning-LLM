WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
),
cleaned AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        INITCAP(TRIM(plan)) AS plan_title
    FROM exploded
    WHERE currency NOT IN ('USD', 'EUR')
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val) AS val_dense_rank,
        CUME_DIST() OVER (ORDER BY val NULLS FIRST) AS val_cume_dist,
        PERCENT_RANK() OVER w_ordered AS val_percent_rank
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY currency ORDER BY val DESC)
),
base AS (
    SELECT *, CAST(val_dense_rank AS DOUBLE) AS val_dense_rank_num
    FROM with_windows
    WHERE val_cume_dist > 0.8
)
SELECT
    *,
    RANK() OVER w_hist AS val_rank,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_cume_dist DESC NULLS LAST) AS val_cume_dist_percent_rank
FROM base
WINDOW w_hist AS (ORDER BY val NULLS FIRST);
