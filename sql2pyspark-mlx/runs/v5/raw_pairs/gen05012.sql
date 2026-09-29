WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS element
)
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    plan,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
    SUM(CASE WHEN plan = 'basic' THEN element ELSE 0 END) AS basic_element,
    COUNT(*) AS num_rows
FROM lines
GROUP BY DATE_TRUNC('YEAR', effective_from), plan
