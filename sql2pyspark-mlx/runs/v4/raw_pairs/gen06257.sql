WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
)
SELECT
    currency,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    MIN(element) AS min_element,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM lines
GROUP BY currency
HAVING COUNT(*) FILTER (WHERE currency = 'THB') < 1
ORDER BY currency DESC NULLS FIRST;
