WITH parsed AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_prices,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    *,
    MIN(entry_price) OVER (PARTITION BY currency) AS currency_min_entry_price,
    RANK() OVER w_ordered AS entry_price_rank
FROM parsed
WINDOW w_ordered AS (PARTITION BY currency ORDER BY entry_price NULLS LAST);
