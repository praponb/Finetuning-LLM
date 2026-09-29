SELECT
    id,
    starts_on,
    discount_pct,
    AVG(discount_pct) OVER (PARTITION BY channel, promo_code) AS channel_avg_discount_pct,
    SUM(discount_pct) OVER w_running AS running_discount_pct
FROM promotions
WINDOW w_running AS (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
