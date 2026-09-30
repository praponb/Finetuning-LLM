WITH lines AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
valid AS (
    SELECT *, CASE WHEN tier_value >= 73 THEN 'High' WHEN tier_value >= 29 THEN 'Medium' ELSE 'Low' END AS tier_value_level
    FROM lines
    WHERE tier_value >= 46
),
filtered AS (
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM valid
    WHERE tier_value > 44
),
kept_keys AS (
    SELECT id
    FROM filtered
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE MONTH(effective_from) IN (1, 7, 12)
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT plan, tier_value
FROM kept_rows
ORDER BY tier_value DESC NULLS LAST;
