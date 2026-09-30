WITH recent_orders AS (
    SELECT *, amount + shipping_fee AS amount_with_shipping
    FROM orders
    WHERE (amount <= 700 AND customer_id IS NULL) OR customer_id IS NOT NULL
),
summary AS (
    SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, channel, MIN(amount_with_shipping) AS min_amount_with_shipping
    FROM recent_orders
    GROUP BY DATE_FORMAT(order_date, 'yyyy-MM'), channel
)
SELECT *
FROM summary
WHERE min_amount_with_shipping >= 56
ORDER BY min_amount_with_shipping DESC
