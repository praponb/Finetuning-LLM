SELECT
    id,
    channel,
    promo_code,
    ends_on,
    LAG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS prev_discount_pct,
    ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS seq_no,
    SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct
FROM promotions
WHERE ends_on <= '2025-05-01';
