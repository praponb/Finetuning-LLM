WITH base AS (
    SELECT *, LEAST(amount, shipping_fee) AS least_amount_shipping_fee
    FROM orders
    WHERE (amount >= 720 AND amount BETWEEN 520 AND 1010) OR shipping_fee >= 17
),
agg AS (
    SELECT channel, ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web, MIN(order_date) AS min_order_date
    FROM base
    GROUP BY channel
)
SELECT channel, pct_web
FROM agg
WHERE pct_web > 47
