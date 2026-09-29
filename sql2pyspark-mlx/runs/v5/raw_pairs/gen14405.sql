WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    currency,
    AVG(entry_price) OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS entry_price_avg_3_prior,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY entry_price, id) AS event_seq,
    NTILE(3) OVER (PARTITION BY currency ORDER BY entry_price, id) AS entry_price_bucket,
    LAST_VALUE(entry_price) IGNORE NULLS OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_entry_price,
    SUM(entry_price) OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_entry_price
FROM unpacked
WHERE entry_price <= 24;
