WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
cleaned AS (
    SELECT *, COALESCE(element, 0) element_filled
    FROM exploded
    WHERE effective_from >= DATE '2024-07-03'
      AND plan = 'basic'
),
filtered AS (
    SELECT *, DATE_TRUNC('month', effective_from) effective_from_period
    FROM cleaned
    WHERE plan IN ('basic', 'pro', 'starter')
)
SELECT
    DATE_TRUNC('QUARTER', effective_from_period) AS quarter_start,
    plan,
    SUM(CASE WHEN plan = 'enterprise' THEN element ELSE 0 END) AS enterprise_element,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    SUM(element_filled) FILTER (WHERE plan = 'basic') AS basic_total_element_filled,
    COUNT(*) FILTER (WHERE plan = 'pro') AS pro_rows,
    SUM(CASE WHEN plan = 'starter' THEN element ELSE 0 END) AS starter_element,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
FROM filtered
GROUP BY CUBE (DATE_TRUNC('QUARTER', effective_from_period), plan)
HAVING SUM(CASE WHEN plan = 'enterprise' THEN element ELSE 0 END) >= 0;
