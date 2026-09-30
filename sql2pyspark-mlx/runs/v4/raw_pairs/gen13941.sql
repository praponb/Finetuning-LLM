SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    status,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
GROUP BY 1, 2
ORDER BY stddev_discount ASC, order_month ASC, status ASC
LIMIT 50;
