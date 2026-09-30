WITH lines AS (
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
SELECT
    *,
    MIN(element) OVER (PARTITION BY currency) AS currency_min_element,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_cume_dist,
    SUM(element) OVER (PARTITION BY currency) AS currency_sum_element
FROM lines;
