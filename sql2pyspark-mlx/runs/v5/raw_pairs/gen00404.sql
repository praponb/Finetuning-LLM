SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    LEAD(discount_pct) OVER w_part AS next_discount_pct,
    LAG(discount_pct) OVER w_part AS prev_discount_pct,
    ROW_NUMBER() OVER w_part AS event_seq,
    SUM(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
FROM promotions
WINDOW w_part AS (PARTITION BY promo_code ORDER BY ends_on, id)
