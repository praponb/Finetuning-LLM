SELECT order_quarter, avg_amount
FROM (
    SELECT QUARTER(order_date) AS order_quarter, ROUND(AVG(amount), 2) AS avg_amount
    FROM orders
    GROUP BY QUARTER(order_date)
) AS agg
WHERE avg_amount < 54
ORDER BY avg_amount DESC, order_quarter
LIMIT 10;
