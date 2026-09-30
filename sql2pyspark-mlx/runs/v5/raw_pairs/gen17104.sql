SELECT *
FROM (
    SELECT status, ROUND(AVG(amount), 2) AS avg_amount, MAX(order_date) AS latest_order_date
    FROM orders
    GROUP BY status
) AS s
WHERE avg_amount >= 52
