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
    effective_from,
    currency,
    COUNT(element) OVER w_ordered AS plan_count_element,
    MAX(element) OVER w_ordered AS plan_max_element
FROM lines
WINDOW w_ordered AS (PARTITION BY plan);
