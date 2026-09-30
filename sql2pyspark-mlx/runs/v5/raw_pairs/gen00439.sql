WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        currency,
        tier_prices,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
SELECT
    id,
    avg_tier_price,
    entry_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY entry_price DESC, id) AS row_seq,
    LAG(lowest_price, 1) OVER (PARTITION BY currency ORDER BY entry_price, id) AS prev_lowest_price
FROM flattened_attrs
