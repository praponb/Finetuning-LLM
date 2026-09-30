WITH flattened_attrs AS (
    SELECT
        id,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    *,
    AVG(entry_price) OVER (ORDER BY entry_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS entry_price_avg_3_prior,
    ROW_NUMBER() OVER (ORDER BY entry_price DESC, id) AS seq_no,
    NTILE(5) OVER (ORDER BY entry_price, id) AS entry_price_bucket
FROM flattened_attrs
