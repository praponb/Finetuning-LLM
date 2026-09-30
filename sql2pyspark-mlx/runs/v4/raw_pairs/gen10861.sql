SELECT order_month, web_count
FROM (
    SELECT MONTH(order_date) AS order_month, SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count
    FROM orders
    GROUP BY MONTH(order_date)
) AS x
WHERE web_count > 60
ORDER BY web_count DESC, order_month DESC;
