SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
    FIRST_VALUE(channel) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
FROM promotions;
