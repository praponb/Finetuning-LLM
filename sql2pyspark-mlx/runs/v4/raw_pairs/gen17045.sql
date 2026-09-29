SELECT status, QUARTER(order_date) AS order_quarter, ROUND(AVG(discount), 2) AS avg_discount
FROM orders
GROUP BY 1, 2
ORDER BY avg_discount DESC, status;
