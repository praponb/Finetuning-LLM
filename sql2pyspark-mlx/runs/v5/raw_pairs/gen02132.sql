WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        effective_from,
        currency,
        tier_prices,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    avg_tier_price,
    AVG(avg_tier_price) OVER (PARTITION BY currency ORDER BY avg_tier_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_tier_price_avg_2,
    FIRST_VALUE(avg_tier_price) IGNORE NULLS OVER (PARTITION BY currency ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_avg_tier_price,
    LAST_VALUE(avg_tier_price) IGNORE NULLS OVER w_grp AS last_known_avg_tier_price,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY avg_tier_price DESC NULLS LAST) AS avg_tier_price_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY avg_tier_price, id) AS event_seq
FROM flattened_attrs
WINDOW w_grp AS (PARTITION BY currency ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
