WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS val
),
cleaned AS (
    SELECT
        *,
        CEIL(val) val_whole,
        ROUND(val * 1.15, 2) val_adjusted
    FROM exploded
    WHERE currency IN ('THB', 'EUR')
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER w_hist val_adjusted_dense_rank,
        DENSE_RANK() OVER (ORDER BY val_whole DESC NULLS LAST) val_whole_dense_rank
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY currency ORDER BY val_adjusted DESC NULLS FIRST)
),
summary AS (
    SELECT
        plan,
        CASE WHEN val_whole_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS val_whole_dense_rank_tier,
        CASE WHEN val_whole >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_whole_tier,
        COUNT(*) AS row_count,
        COUNT(*) AS row_count_2,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
        ROUND(SUM(val_whole) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS val_whole_per_usd,
        ROUND(SUM(val_adjusted_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS val_adjusted_dense_rank_per_pro
    FROM scored
    GROUP BY GROUPING SETS ((plan, CASE WHEN val_whole_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN val_whole >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END), (plan, CASE WHEN val_whole_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    val_whole_tier,
    row_count,
    row_count_2,
    num_plans,
    thb_rows,
    val_adjusted_dense_rank_per_pro
FROM summary
ORDER BY num_plans DESC NULLS FIRST, row_count_2
