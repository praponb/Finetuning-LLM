WITH flattened_attrs AS (
    SELECT
        id,
        effective_from,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    ROW_NUMBER() OVER w_hist AS seq_no,
    ROW_NUMBER() OVER w_hist AS row_seq,
    ROW_NUMBER() OVER w_hist AS event_seq
FROM flattened_attrs
WINDOW w_hist AS (ORDER BY effective_from, id);
