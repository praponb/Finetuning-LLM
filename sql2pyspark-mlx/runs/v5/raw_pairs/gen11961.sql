SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') order_month,
    ROUND(AVG(discount), 2) avg_discount,
    ROUND(STDDEV(amount), 2) stddev_amount,
    COUNT(amount) amount_count
FROM orders
GROUP BY order_month
HAVING SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) > 28
ORDER BY amount_count
