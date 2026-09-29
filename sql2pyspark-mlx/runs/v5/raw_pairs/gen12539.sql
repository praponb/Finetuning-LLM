SELECT *
FROM (
    SELECT status, COUNT(*) AS cnt, MAX(order_date) AS last_order_date
    FROM orders
    WHERE shipping_fee >= 17
    GROUP BY status
) AS s
WHERE cnt >= 10
ORDER BY last_order_date
