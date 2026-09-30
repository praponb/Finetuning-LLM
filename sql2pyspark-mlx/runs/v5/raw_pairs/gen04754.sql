WITH lines AS (
    SELECT
        id,
        plan,
        EXPLODE(discounts) AS element
    FROM price_tiers
)
SELECT
    CASE WHEN element >= 89 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    plan,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(element, 0.5) AS element_p50,
    SUM(CASE WHEN plan = 'starter' THEN element ELSE 0 END) AS starter_element
FROM lines
GROUP BY CASE WHEN element >= 89 THEN 'LARGE' ELSE 'SMALL' END, plan
HAVING SUM(CASE WHEN plan = 'starter' THEN element ELSE 0 END) < 1000000
