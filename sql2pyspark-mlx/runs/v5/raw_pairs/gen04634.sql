WITH flattened AS (
    SELECT
        id,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT
    currency,
    MAX(tier_value) FILTER (WHERE currency = 'THB') AS thb_max_tier_value,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles
FROM flattened
GROUP BY currency;
