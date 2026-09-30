WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS val
),
cleaned AS (
    SELECT *, DAY(effective_from) AS effective_from_day
    FROM flattened
    WHERE plan IN ('basic', 'enterprise')
),
filtered AS (
    SELECT *, effective_from_day - val AS effective_from_day_minus_val
    FROM cleaned
    WHERE plan IN ('basic', 'starter')
),
metrics AS (
    SELECT
        *,
        MIN(effective_from_day_minus_val) OVER (PARTITION BY plan) AS plan_min_effective_from_day_minus_val,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        RANK() OVER (PARTITION BY plan ORDER BY effective_from_day_minus_val DESC) AS effective_from_day_minus_val_rank
    FROM filtered
)
SELECT
    id,
    plan,
    effective_from,
    val,
    effective_from_day,
    effective_from_day_minus_val,
    plan_min_effective_from_day_minus_val,
    plan_count_val,
    effective_from_day_minus_val_rank,
    CASE WHEN plan RLIKE '^(basic|enterprise)$' THEN 'GROUP_A' WHEN plan RLIKE '^(starter|pro)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS plan_zone
FROM metrics
