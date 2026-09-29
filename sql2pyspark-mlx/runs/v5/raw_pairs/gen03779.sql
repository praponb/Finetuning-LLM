WITH flattened AS (
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS val
)
SELECT
    id,
    plan,
    RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_rank,
    SUM(val) OVER (PARTITION BY plan) AS plan_sum_val
FROM flattened;
