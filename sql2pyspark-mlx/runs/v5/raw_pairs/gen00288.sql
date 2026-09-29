SELECT
    id,
    promo_code,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
    FIRST_VALUE(channel) IGNORE NULLS OVER w_part AS first_known_channel
FROM promotions
WHERE ends_on < '2024-02-01'
WINDOW w_part AS (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
