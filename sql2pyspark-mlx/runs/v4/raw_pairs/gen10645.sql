WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
metrics AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        MIN(val) OVER (PARTITION BY plan) AS plan_min_val,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY val DESC) AS val_dense_rank
    FROM lines
),
scored AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        plan_min_val,
        val_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_min_val ASC NULLS LAST) AS plan_min_val_cume_dist,
        MAX(val_dense_rank) OVER (PARTITION BY plan) AS plan_max_val_dense_rank
    FROM metrics
),
filtered AS (
    SELECT
        *,
        CONCAT(currency, ' - ', plan) AS currency_plan,
        CASE WHEN val >= 64 THEN 'High' WHEN val >= 30 THEN 'Medium' ELSE 'Low' END AS val_band
    FROM scored
    WHERE effective_from > TO_DATE('2024-09-23')
),
filtered_price_tiers AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        val_dense_rank,
        plan_min_val_cume_dist,
        currency_plan,
        val_band,
        INITCAP(TRIM(plan)) AS plan_title
    FROM filtered
    WHERE effective_from > TO_DATE('2024-09-18')
      AND plan_max_val_dense_rank > 4
)
SELECT
    currency,
    plan,
    ROUND(SUM(plan_min_val_cume_dist) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS plan_min_val_cume_dist_per_eur,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    ROUND(AVG(plan_min_val_cume_dist), 2) AS avg_plan_min_val_cume_dist,
    ROUND(AVG(plan_min_val_cume_dist), 2) AS avg_plan_min_val_cume_dist_2,
    AVG(val_dense_rank) AS avg_val_dense_rank
FROM filtered_price_tiers
GROUP BY currency, plan
ORDER BY avg_plan_min_val_cume_dist ASC NULLS FIRST
