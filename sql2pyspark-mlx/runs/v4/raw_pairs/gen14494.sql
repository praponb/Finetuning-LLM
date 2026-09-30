WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    id,
    plan,
    effective_from,
    currency,
    val,
    COUNT(val) OVER (PARTITION BY currency, plan) AS currency_count_val,
    RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_rank,
    RANK() OVER (PARTITION BY plan ORDER BY val ASC) AS val_rank_2
FROM flattened
