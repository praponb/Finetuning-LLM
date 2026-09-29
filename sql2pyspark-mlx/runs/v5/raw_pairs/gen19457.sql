WITH exploded AS (
    SELECT
        id,
        plan,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    plan,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    COUNT(DISTINCT plan) AS distinct_plans
FROM exploded
GROUP BY plan
HAVING COUNT(DISTINCT plan) > 1
ORDER BY plan DESC NULLS LAST
