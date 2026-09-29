SELECT order_month, stddev_discount
FROM (
    SELECT MONTH(order_date) AS order_month, ROUND(STDDEV(discount), 2) AS stddev_discount
    FROM orders
    WHERE store_id = 36
    GROUP BY MONTH(order_date)
) AS x
WHERE stddev_discount >= 35
ORDER BY stddev_discount, 1 DESC;
