WITH flattened AS (
    SELECT id, EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
base AS (
    SELECT
        *,
        ROUND(tier_value, 1) tier_value_rounded,
        CAST(tier_value AS STRING) tier_value_str
    FROM flattened
    WHERE tier_value BETWEEN 23 AND 79
),
metrics AS (
    SELECT
        id,
        tier_value,
        tier_value_rounded,
        RANK() OVER w_hist tier_value_rank
    FROM base
    WINDOW w_hist AS (ORDER BY tier_value)
),
enriched AS (
    SELECT *, PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) tier_value_percent_rank
    FROM metrics
)
SELECT id, tier_value_rank
FROM enriched
ORDER BY id DESC;
