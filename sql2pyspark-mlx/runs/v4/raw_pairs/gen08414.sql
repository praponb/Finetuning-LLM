WITH flattened AS (
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS element
)
SELECT
    id,
    plan,
    currency,
    element,
    AVG(element) OVER w_part AS plan_avg_element,
    RANK() OVER (ORDER BY element) AS element_rank,
    MAX(element) OVER (PARTITION BY currency) AS currency_max_element,
    PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS LAST) AS element_percent_rank,
    MAX(element) OVER w_recent AS currency_max_element_2
FROM flattened
WINDOW w_part AS (PARTITION BY plan), w_recent AS (PARTITION BY currency, plan);
