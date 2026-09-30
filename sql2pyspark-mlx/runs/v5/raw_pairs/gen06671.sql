SELECT id, order_date, amount, SUM(amount) OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount FROM orders ORDER BY 4 DESC;
